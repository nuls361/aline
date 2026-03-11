"""
Aline Email Agent — Autonomous outreach for executive placement signals.

Loads roles from Attio (sales_stage = ready_for_outreach), identifies the
decision maker via Apollo/Tavily, generates a personalized cold email using
Claude + soul.md/skill.md, and sends via Instantly API.
"""

import os
import json
import time
import logging
from datetime import datetime, timezone

import anthropic
import requests
from tavily import TavilyClient

from attio_client import attio

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- Config ---
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
APOLLO_API_KEY = os.environ["APOLLO_API_KEY"]
TAVILY_API_KEY = os.environ["TAVILY_API_KEY"]
INSTANTLY_API_KEY = os.environ["INSTANTLY_API_KEY"]
SLACK_WEBHOOK = os.environ["SLACK_WEBHOOK_HOT_LEADS"]
OUTREACH_DAILY_LIMIT = int(os.environ.get("OUTREACH_DAILY_LIMIT", "10"))
MAX_ITERATIONS = 10

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Clients ---
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
tavily = TavilyClient(api_key=TAVILY_API_KEY)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Aline-Email-Agent/1.0"})

# --- Load system prompt ---
def load_system_prompt():
    with open(os.path.join(BASE_DIR, "soul.md")) as f:
        soul = f.read()
    with open(os.path.join(BASE_DIR, "skill.md")) as f:
        skill = f.read()
    return soul + "\n\n---\n\n" + skill


# --- Tool definitions for Claude ReAct ---
TOOLS = [
    {
        "name": "apollo_people_search",
        "description": "Search Apollo for people at a company by domain and title keywords. Returns list of people with name, title, email, linkedin.",
        "input_schema": {
            "type": "object",
            "properties": {
                "domain": {"type": "string", "description": "Company domain (e.g. acme.de)"},
                "title_keywords": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Title keywords to search for (e.g. ['CEO', 'Founder'])"
                }
            },
            "required": ["domain", "title_keywords"]
        }
    },
    {
        "name": "tavily_search",
        "description": "Search the web for information about a person or company.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "attio_upsert_person",
        "description": "Create or update a Person in Attio CRM.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "email": {"type": "string"},
                "title": {"type": "string"},
                "company_record_id": {"type": "string"}
            },
            "required": ["name", "email"]
        }
    },
    {
        "name": "attio_link_contact_to_role",
        "description": "Link a Person record to a Role record in Attio.",
        "input_schema": {
            "type": "object",
            "properties": {
                "person_record_id": {"type": "string"},
                "role_record_id": {"type": "string"}
            },
            "required": ["person_record_id", "role_record_id"]
        }
    },
]

# --- Tool implementations ---

def apollo_people_search(domain: str, title_keywords: list[str]) -> list[dict]:
    """Search Apollo.io for people at a domain matching title keywords."""
    url = "https://api.apollo.io/v1/mixed_people/search"
    payload = {
        "api_key": APOLLO_API_KEY,
        "q_organization_domains": domain,
        "person_titles": title_keywords,
        "page": 1,
        "per_page": 5,
    }
    try:
        resp = SESSION.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for person in data.get("people", []):
            results.append({
                "name": person.get("name", ""),
                "title": person.get("title", ""),
                "email": person.get("email", ""),
                "linkedin_url": person.get("linkedin_url", ""),
                "organization": person.get("organization", {}).get("name", ""),
            })
        return results
    except Exception as e:
        log.error(f"Apollo search error: {e}")
        return []


def tavily_search_tool(query: str) -> list[dict]:
    """Search the web via Tavily."""
    try:
        resp = tavily.search(query=query, max_results=5, days=7)
        return [
            {
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "snippet": r.get("content", ""),
            }
            for r in resp.get("results", [])
        ]
    except Exception as e:
        log.error(f"Tavily search error: {e}")
        return []


def attio_upsert_person(name: str, email: str, title: str = "", company_record_id: str = "") -> dict:
    """Upsert a person in Attio."""
    values = {}
    if title:
        values["job_title"] = [{"value": title}]
    if company_record_id:
        values["company"] = attio.format_record_reference("companies", company_record_id)

    resp = attio.upsert_person(email=email, values=values)
    if resp:
        record_id = attio.extract_record_id(resp.get("data", resp))
        return {"record_id": record_id, "status": "ok"}
    return {"record_id": None, "status": "failed"}


def attio_link_contact_to_role(person_record_id: str, role_record_id: str) -> dict:
    """Link a person to a role in Attio via a note."""
    resp = attio.create_note(
        parent_object="roles",
        parent_record_id=role_record_id,
        title="Decision maker identified",
        content=f"Linked person {person_record_id} as decision maker.",
    )
    return {"status": "ok" if resp else "failed"}


TOOL_MAP = {
    "apollo_people_search": lambda **kw: apollo_people_search(**kw),
    "tavily_search": lambda **kw: tavily_search_tool(**kw),
    "attio_upsert_person": lambda **kw: attio_upsert_person(**kw),
    "attio_link_contact_to_role": lambda **kw: attio_link_contact_to_role(**kw),
}


def execute_tools(content_blocks) -> list[dict]:
    """Execute tool calls from Claude response."""
    results = []
    for block in content_blocks:
        if block.type == "tool_use":
            fn = TOOL_MAP.get(block.name)
            if fn:
                output = fn(**block.input)
            else:
                output = {"error": f"Unknown tool: {block.name}"}
            results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": json.dumps(output),
            })
    return results


# --- Claude API call with retry ---
def call_claude(system_prompt: str, messages: list, tools=None, max_tokens: int = 4096):
    for attempt in range(3):
        try:
            kwargs = {
                "model": "claude-sonnet-4-5-20250929",
                "max_tokens": max_tokens,
                "system": system_prompt,
                "messages": messages,
            }
            if tools:
                kwargs["tools"] = tools
            return claude.messages.create(**kwargs)
        except Exception as e:
            if attempt == 2:
                raise
            wait = 15 * (attempt + 1)
            log.warning(f"Claude API error (attempt {attempt+1}): {e}. Retrying in {wait}s...")
            time.sleep(wait)


# --- Decision maker reasoning ---

DECISION_MAKER_PROMPT = """You are Aline's outreach intelligence agent. Your job is to identify
the right decision maker for an executive placement engagement.

Role: {role_title} ({engagement_type})
Company: {company_name} (domain: {company_domain})
Signal: {signal_type} — {signal_summary}

REASONING RULES:
- Funding round (Series A/B/C) → CEO or COO is primary buyer.
  Rationale: founders are in growth mode, need operators fast.
- Restructuring / Insolvency → CFO or managing director.
  Rationale: financial control is the pain point.
- International Expansion → COO or Country Manager / Head of Operations.
- Leadership departure / C-Level vacancy → Board contact or CEO.
- PE deal / acquisition → CFO + CEO both relevant.
- Small company (<30 employees): CEO is almost always the decision maker.
- Large company (>200 employees): function-specific VP/Director may be buyer.

Use the tools available to find the right person at this company.
Search Apollo first by domain + inferred title keywords.
If Apollo returns no results, use tavily_search to find the person on LinkedIn.
Once found, upsert to Attio and link to the role.

Return your reasoning as a short explanation before calling tools."""


def find_decision_maker(role: dict, company_domain: str) -> dict | None:
    """Run ReAct loop to find the decision maker for a role."""
    role_title = attio.extract_value(role, "name", "Unknown Role")
    engagement_type = attio.extract_value(role, "engagement_type", "")
    company_name = attio.extract_value(role, "company_name", "Unknown Company")
    signal_type = attio.extract_value(role, "signal_type", "")
    signal_summary = attio.extract_value(role, "signal_summary", "")

    prompt = DECISION_MAKER_PROMPT.format(
        role_title=role_title,
        engagement_type=engagement_type,
        company_name=company_name,
        company_domain=company_domain,
        signal_type=signal_type,
        signal_summary=signal_summary,
    )

    system_prompt = load_system_prompt()
    messages = [{"role": "user", "content": prompt}]

    for iteration in range(MAX_ITERATIONS):
        if iteration > 0:
            time.sleep(5)
        response = call_claude(system_prompt, messages, tools=TOOLS)

        if response.stop_reason == "end_turn":
            # Extract decision maker info from final text
            for block in response.content:
                if hasattr(block, "text"):
                    text = block.text.strip()
                    try:
                        start = text.find("{")
                        end = text.rfind("}")
                        if start != -1 and end != -1:
                            return json.loads(text[start:end+1])
                    except json.JSONDecodeError:
                        pass
            return None

        if response.stop_reason == "tool_use":
            tool_results = execute_tools(response.content)
            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})
            continue

        log.warning(f"Unexpected stop_reason: {response.stop_reason}")
        break

    return None


# --- Email generation ---

EMAIL_PROMPT = """{soul_md}
{skill_md}

Write a cold outreach email to {decision_maker_name}, {decision_maker_title} at {company_name}.

Context:
- We spotted this signal: {signal_type} — {signal_summary}
- We're reaching out because we place Fractional/Interim {role_function} executives
  into DACH startups. We're not recruiters. We're operators.
- The role we see a fit for: {role_title}

Rules:
- Max 5 sentences in the body. No fluff.
- Opening line must reference the specific signal (not generic).
- No "I hope this finds you well". No "Dear Sir/Madam".
- CTA: one clear question or ask. Never "let me know if you're interested."
- Subject line: max 8 words, no clickbait.
- Language: English unless company is clearly German-only (check domain/name).

Return JSON:
{{
  "subject": "...",
  "body": "...",
  "reasoning": "why this angle for this signal type"
}}"""


def generate_email(role: dict, decision_maker: dict) -> dict | None:
    """Generate a cold outreach email using Claude."""
    with open(os.path.join(BASE_DIR, "soul.md")) as f:
        soul_md = f.read()
    with open(os.path.join(BASE_DIR, "skill.md")) as f:
        skill_md = f.read()

    role_title = attio.extract_value(role, "name", "")
    role_function = attio.extract_value(role, "role_function", "")
    company_name = attio.extract_value(role, "company_name", "")
    signal_type = attio.extract_value(role, "signal_type", "")
    signal_summary = attio.extract_value(role, "signal_summary", "")

    prompt = EMAIL_PROMPT.format(
        soul_md=soul_md,
        skill_md=skill_md,
        decision_maker_name=decision_maker.get("name", ""),
        decision_maker_title=decision_maker.get("title", ""),
        company_name=company_name,
        signal_type=signal_type,
        signal_summary=signal_summary,
        role_function=role_function,
        role_title=role_title,
    )

    response = call_claude(
        system_prompt="You are Aline's email copywriter. Return only valid JSON.",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1024,
    )

    for block in response.content:
        if hasattr(block, "text"):
            text = block.text.strip()
            start = text.find("{")
            end = text.rfind("}")
            if start != -1 and end != -1:
                try:
                    return json.loads(text[start:end+1])
                except json.JSONDecodeError:
                    pass
    return None


# --- Instantly API ---

def send_via_instantly(email: str, name: str, company: str, subject: str, body: str) -> bool:
    """Add lead to Instantly campaign and send."""
    base_url = "https://api.instantly.ai/api/v1"

    # Add lead
    try:
        resp = SESSION.post(
            f"{base_url}/lead/add",
            json={
                "api_key": INSTANTLY_API_KEY,
                "email": email,
                "first_name": name.split()[0] if name else "",
                "last_name": " ".join(name.split()[1:]) if name and len(name.split()) > 1 else "",
                "company_name": company,
                "custom_variables": {
                    "subject": subject,
                    "body": body,
                },
            },
            timeout=15,
        )
        if resp.status_code == 429:
            log.warning("Instantly rate limited, sleeping 60s")
            time.sleep(60)
            resp = SESSION.post(
                f"{base_url}/lead/add",
                json={
                    "api_key": INSTANTLY_API_KEY,
                    "email": email,
                    "first_name": name.split()[0] if name else "",
                    "last_name": " ".join(name.split()[1:]) if name and len(name.split()) > 1 else "",
                    "company_name": company,
                },
                timeout=15,
            )
        resp.raise_for_status()
        log.info(f"Instantly: added lead {email}")
        return True
    except Exception as e:
        log.error(f"Instantly error: {e}")
        return False


# --- Slack ---

def send_slack(text: str):
    try:
        resp = requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"Slack error: {e}")


def format_outreach_alert(role_title: str, company: str, dm_name: str, subject: str) -> str:
    return (
        f"\U0001f4e7 *Outreach sent — {company}*\n"
        f"Role: {role_title}\n"
        f"To: {dm_name}\n"
        f"Subject: {subject}"
    )


# --- Main ---

def main():
    # Load roles ready for outreach
    roles = attio.query_records("roles", filter={
        "sales_stage": {"$eq": "ready_for_outreach"},
    })

    if not roles:
        log.info("No roles ready for outreach.")
        return

    log.info(f"Found {len(roles)} roles ready for outreach (limit: {OUTREACH_DAILY_LIMIT})")
    emails_sent = 0

    for role in roles[:OUTREACH_DAILY_LIMIT]:
        role_id = attio.extract_record_id(role)
        role_title = attio.extract_value(role, "name", "Unknown Role")
        company_name = attio.extract_value(role, "company_name", "Unknown")

        log.info(f"Processing: {role_title} at {company_name}")

        # Get company domain
        company_domain = attio.extract_value(role, "company_domain", "")
        if not company_domain:
            log.warning(f"No domain for {company_name}, skipping")
            continue

        try:
            # Step 1: Find decision maker
            dm = find_decision_maker(role, company_domain)
            if not dm or not dm.get("email"):
                log.warning(f"No decision maker email found for {company_name}")
                attio.create_note(
                    parent_object="roles",
                    parent_record_id=role_id,
                    title="Outreach skipped",
                    content="No decision maker email found via Apollo or Tavily.",
                )
                continue

            log.info(f"Decision maker: {dm.get('name', '?')} ({dm.get('email', '?')})")

            # Step 2: Generate email
            email_data = generate_email(role, dm)
            if not email_data:
                log.error(f"Email generation failed for {company_name}")
                continue

            subject = email_data["subject"]
            body = email_data["body"]
            log.info(f"Email generated: {subject}")

            # Step 3: Send via Instantly
            sent = send_via_instantly(
                email=dm["email"],
                name=dm.get("name", ""),
                company=company_name,
                subject=subject,
                body=body,
            )

            if not sent:
                log.error(f"Failed to send email for {company_name}")
                continue

            # Step 4: Log to Attio
            attio.create_note(
                parent_object="roles",
                parent_record_id=role_id,
                title="Outreach sent",
                content=f"To: {dm.get('name', '')} <{dm['email']}>\nSubject: {subject}\n\n{body}",
            )

            # Step 5: Update sales stage
            attio.update_role(role_id, {
                "sales_stage": attio.format_select("sdr_contacted"),
            })

            # Step 6: Slack alert
            send_slack(format_outreach_alert(role_title, company_name, dm.get("name", ""), subject))

            emails_sent += 1
            log.info(f"Outreach complete for {company_name} ({emails_sent}/{OUTREACH_DAILY_LIMIT})")

        except Exception as e:
            log.error(f"Error processing {company_name}: {e}")
            continue

    # Summary
    now = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M")
    send_slack(f"\u2705 *Email Agent — {now}*\nEmails sent: {emails_sent}/{len(roles[:OUTREACH_DAILY_LIMIT])}")
    log.info(f"Done. Emails sent: {emails_sent}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"Fatal error: {e}")
        try:
            send_slack(f"\u26a0\ufe0f *Email Agent Error*\n{e}")
        except Exception:
            pass
        raise
