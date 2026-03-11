"""
Aline Dry Run — Test the full outreach pipeline locally.

Paste a JD URL, see the entire pipeline output in terminal.
No Attio writes. No emails sent. No Slack messages.

Usage:
  python dry_run.py --url "https://jobs.ashbyhq.com/somecompany/cfo-interim"
  python dry_run.py --url "https://www.linkedin.com/jobs/view/123456789" --verbose
  python dry_run.py --url "..." --no-apollo
  python dry_run.py --url "..." --no-apollo --no-tavily  # skip Tavily (SSL fix for macOS Python 3.9)
"""

import os
import sys
import json
import argparse
import logging
import time

import anthropic
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- Config ---
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY", "")
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY", "")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

claude = None  # initialized in main()

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
})

VERBOSE = False
SKIP_TAVILY = False

# --- Helpers ---

def p(emoji: str, msg: str):
    """Print with emoji prefix."""
    print(f"{emoji} {msg}")


def p_verbose(label: str, data):
    """Print only in verbose mode."""
    if VERBOSE:
        print(f"  [DEBUG] {label}: {json.dumps(data, indent=2, default=str)[:2000]}")


# --- Step 1: Fetch JD ---

def fetch_jd(url: str) -> dict:
    """Fetch and parse a job description from URL."""
    p("\U0001f4c4", f"Fetching JD from: {url}")

    try:
        resp = SESSION.get(url, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        p("\u274c", f"Failed to fetch URL: {e}")
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")

    # Try structured data first
    title = ""
    company = ""
    location = ""
    description = ""

    # JSON-LD
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            ld = json.loads(script.string)
            if isinstance(ld, dict) and ld.get("@type") == "JobPosting":
                title = ld.get("title", "")
                org = ld.get("hiringOrganization", {})
                company = org.get("name", "") if isinstance(org, dict) else ""
                loc = ld.get("jobLocation", {})
                if isinstance(loc, list) and loc:
                    loc = loc[0]
                if isinstance(loc, dict):
                    addr = loc.get("address", {})
                    if isinstance(addr, dict):
                        location = f"{addr.get('addressLocality', '')}, {addr.get('addressCountry', '')}".strip(", ")
                description = ld.get("description", "")[:3000]
                break
        except (json.JSONDecodeError, KeyError):
            continue

    # Fallback: HTML parsing
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""

    if not company:
        # Try meta tags or og tags
        meta = soup.find("meta", {"property": "og:site_name"})
        company = meta["content"] if meta and meta.get("content") else ""

    if not description:
        # Try common job description containers
        for selector in [".job-description", "[data-testid='job-description']",
                        ".posting-page", "article", ".content"]:
            el = soup.select_one(selector)
            if el:
                description = el.get_text(" ", strip=True)[:3000]
                break
        if not description:
            body = soup.find("body")
            description = body.get_text(" ", strip=True)[:3000] if body else ""

    p("\U0001f4c4", f"JD fetched: {title or '(no title)'} at {company or '(no company)'}")
    if location:
        p("  ", f"Location: {location}")

    result = {
        "title": title,
        "company": company,
        "location": location,
        "description": description,
        "url": url,
    }
    p_verbose("JD data", {k: v[:200] if isinstance(v, str) else v for k, v in result.items()})
    return result


# --- Step 2: Enrich company ---

def enrich_company(company_name: str) -> dict:
    """Enrich company info via Tavily."""
    p("\U0001f3e2", f"Enriching company: {company_name}")

    results = []
    if SKIP_TAVILY:
        p("⏭️", "Skipping Tavily (--no-tavily flag) — Claude-only enrichment")
    else:
        from tavily import TavilyClient
        tavily = TavilyClient(api_key=TAVILY_API_KEY)

        try:
            resp = tavily.search(
                query=f"{company_name} company domain website funding employees DACH",
                max_results=3,
                days=30,
            )
            results = resp.get("results", [])
            p_verbose("Tavily company results", results)
        except Exception as e:
            p("\u26a0\ufe0f", f"Tavily search failed: {e}")
            results = []

    # Use Claude to extract structured info
    context = "\n".join([
        f"- {r.get('title', '')}: {r.get('content', '')[:300]}"
        for r in results
    ])

    try:
        response = claude.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=512,
            system="Extract company info. Return only valid JSON.",
            messages=[{"role": "user", "content": f"""From these search results about "{company_name}", extract:
- domain (company website domain, e.g. acme.de)
- headcount_estimate (number or "unknown")
- funding_stage (e.g. "Series A", "Series B", "Bootstrapped", "unknown")
- one_liner (what the company does, max 10 words)

Search results:
{context}

Return JSON: {{"domain": "...", "headcount": "...", "funding_stage": "...", "one_liner": "..."}}"""}],
        )
        for block in response.content:
            if hasattr(block, "text"):
                text = block.text.strip()
                start = text.find("{")
                end = text.rfind("}")
                if start != -1 and end != -1:
                    info = json.loads(text[start:end+1])
                    p("\U0001f3e2", f"Company: {company_name} | Domain: {info.get('domain', '?')} | "
                      f"Size: ~{info.get('headcount', '?')} | Stage: {info.get('funding_stage', '?')}")
                    p("  ", f"What they do: {info.get('one_liner', '?')}")
                    return info
    except Exception as e:
        p("\u26a0\ufe0f", f"Claude enrichment failed: {e}")

    return {"domain": "", "headcount": "unknown", "funding_stage": "unknown"}


# --- Step 3: Classify role ---

def classify_role(jd: dict) -> dict:
    """Use Claude to classify the role."""
    p("\U0001f3f7\ufe0f", "Classifying role...")

    try:
        response = claude.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=512,
            system="Classify this job. Return only valid JSON.",
            messages=[{"role": "user", "content": f"""Classify this role:
Title: {jd.get('title', '')}
Company: {jd.get('company', '')}
Description excerpt: {jd.get('description', '')[:1500]}

Return JSON:
{{
  "engagement_type": "Interim" or "Fractional" or "Full-time",
  "role_function": "Finance" or "Technology" or "Operations" or "People" or "Product" or "Marketing" or "Sales" or "General Management",
  "signal_type": infer the likely signal (e.g. "Leadership Departure", "Funding Round", "Restructuring", "Growth Hire", "International Expansion"),
  "reasoning": "one sentence explaining your classification"
}}"""}],
        )
        for block in response.content:
            if hasattr(block, "text"):
                text = block.text.strip()
                start = text.find("{")
                end = text.rfind("}")
                if start != -1 and end != -1:
                    info = json.loads(text[start:end+1])
                    p("\U0001f3f7\ufe0f", f"Role type: {info.get('engagement_type', '?')} | "
                      f"Function: {info.get('role_function', '?')} | "
                      f"Signal inferred: {info.get('signal_type', '?')}")
                    if info.get("reasoning"):
                        p("  ", f"Reasoning: {info['reasoning']}")
                    return info
    except Exception as e:
        p("\u274c", f"Classification failed: {e}")

    return {"engagement_type": "Unknown", "role_function": "Unknown", "signal_type": "Unknown"}


# --- Step 4: Find decision maker ---

def find_decision_maker(company_name: str, domain: str, role_info: dict,
                        skip_apollo: bool = False) -> dict:
    """Find the decision maker via Apollo and/or Tavily."""
    p("\U0001f9e0", "Reasoning about decision maker...")

    signal = role_info.get("signal_type", "")
    engagement = role_info.get("engagement_type", "")
    role_function = role_info.get("role_function", "")

    # Claude reasoning for who to target
    try:
        response = claude.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=512,
            system="You are Aline's outreach intelligence agent.",
            messages=[{"role": "user", "content": f"""Who is the right decision maker to contact about placing a {engagement} {role_function} executive?

Company: {company_name} (domain: {domain})
Signal: {signal}

REASONING RULES:
- Funding round → CEO or COO is primary buyer
- Restructuring / Insolvency → CFO or managing director
- International Expansion → COO or Country Manager
- Leadership departure → Board contact or CEO
- PE deal / acquisition → CFO + CEO both relevant
- Small company (<30): CEO is almost always the decision maker

Return JSON:
{{
  "target_titles": ["CEO", "Founder"],
  "reasoning": "one sentence why these titles"
}}"""}],
        )
        for block in response.content:
            if hasattr(block, "text"):
                text = block.text.strip()
                start = text.find("{")
                end = text.rfind("}")
                if start != -1 and end != -1:
                    targeting = json.loads(text[start:end+1])
                    p("\U0001f9e0", f"Reasoning: {targeting.get('reasoning', '')}")
                    p("\U0001f50d", f"Target titles: {targeting.get('target_titles', [])}")
                    break
        else:
            targeting = {"target_titles": ["CEO", "Founder"]}
    except Exception as e:
        p("\u26a0\ufe0f", f"Reasoning failed: {e}")
        targeting = {"target_titles": ["CEO", "Founder"]}

    # Apollo search
    if not skip_apollo and APOLLO_API_KEY and domain:
        p("\U0001f50d", f"Apollo search: domain={domain}, titles={targeting['target_titles']}")
        try:
            resp = SESSION.post(
                "https://api.apollo.io/v1/mixed_people/search",
                json={
                    "api_key": APOLLO_API_KEY,
                    "q_organization_domains": domain,
                    "person_titles": targeting["target_titles"],
                    "page": 1,
                    "per_page": 5,
                },
                timeout=15,
            )
            resp.raise_for_status()
            people = resp.json().get("people", [])
            p_verbose("Apollo results", people)

            if people:
                person = people[0]
                name = person.get("name", "")
                email = person.get("email", "")
                title = person.get("title", "")
                linkedin = person.get("linkedin_url", "")

                if email:
                    p("\u2705", f"Decision maker found: {name}, {title} — {email}")
                else:
                    p("\u26a0\ufe0f", f"Found {name}, {title} but no email. LinkedIn: {linkedin}")

                return {
                    "name": name,
                    "title": title,
                    "email": email,
                    "linkedin_url": linkedin,
                    "source": "Apollo",
                }
            else:
                p("\u26a0\ufe0f", "Apollo returned no results")
        except Exception as e:
            p("\u26a0\ufe0f", f"Apollo search failed: {e}")
    elif skip_apollo:
        p("\u23ed\ufe0f", "Skipping Apollo (--no-apollo flag)")

    # Fallback: Tavily search
    if SKIP_TAVILY:
        p("⏭️", "Skipping Tavily people search (--no-tavily flag)")
        p("⚠️", f"No decision maker found — using placeholder for demo")
        return {
            "name": f"[{targeting['target_titles'][0]}]",
            "title": targeting["target_titles"][0],
            "email": "",
            "linkedin_url": "",
            "source": "placeholder (--no-tavily)",
        }

    p("\U0001f50d", f"Tavily fallback: searching for {targeting['target_titles'][0]} at {company_name}")
    from tavily import TavilyClient
    tavily = TavilyClient(api_key=TAVILY_API_KEY)

    try:
        resp = tavily.search(
            query=f"{company_name} {targeting['target_titles'][0]} LinkedIn",
            max_results=3,
        )
        results = resp.get("results", [])
        p_verbose("Tavily people results", results)

        if results:
            # Use Claude to extract person info
            context = "\n".join([f"- {r.get('title', '')}: {r.get('content', '')[:200]}" for r in results])
            extract_resp = claude.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=256,
                system="Extract person info. Return only valid JSON.",
                messages=[{"role": "user", "content": f"""From these search results, extract the {targeting['target_titles'][0]} of {company_name}:

{context}

Return JSON: {{"name": "...", "title": "...", "linkedin_url": "...", "email": ""}}
If not found, return {{"name": "", "title": "", "linkedin_url": "", "email": ""}}"""}],
            )
            for block in extract_resp.content:
                if hasattr(block, "text"):
                    text = block.text.strip()
                    start = text.find("{")
                    end = text.rfind("}")
                    if start != -1 and end != -1:
                        person = json.loads(text[start:end+1])
                        if person.get("name"):
                            p("\u2705", f"Found via Tavily: {person['name']}, {person.get('title', '')} — LinkedIn: {person.get('linkedin_url', 'N/A')}")
                            if not person.get("email"):
                                p("\u26a0\ufe0f", "No email found via Tavily. LinkedIn only.")
                            person["source"] = "Tavily"
                            return person
    except Exception as e:
        p("\u26a0\ufe0f", f"Tavily people search failed: {e}")

    p("\u274c", "No decision maker found via any source")
    return {"name": "", "title": "", "email": "", "linkedin_url": "", "source": "none"}


# --- Step 5: Generate email ---

def generate_email(jd: dict, company_info: dict, role_info: dict, dm: dict) -> dict:
    """Generate outreach email using Claude + soul.md + skill.md."""
    p("\U0001f4e7", "Generating outreach email...")

    with open(os.path.join(BASE_DIR, "soul.md")) as f:
        soul_md = f.read()
    with open(os.path.join(BASE_DIR, "skill.md")) as f:
        skill_md = f.read()

    try:
        response = claude.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1024,
            system="You are Aline's email copywriter. Return only valid JSON.",
            messages=[{"role": "user", "content": f"""{soul_md}
{skill_md}

Write a cold outreach email to {dm.get('name', 'the decision maker')}, {dm.get('title', '')} at {jd.get('company', '')}.

ENRICHMENT CONTEXT (use this — we did the research):
- Signal spotted: {role_info.get('signal_type', '')}
- Role: {jd.get('title', '')} ({role_info.get('engagement_type', 'Unknown')} position)
- Company: {jd.get('company', '')} — {company_info.get('one_liner', 'unknown')}
- Stage: {company_info.get('funding_stage', 'unknown')} | Size: ~{company_info.get('headcount', 'unknown')}
- Domain: {company_info.get('domain', 'unknown')}
- Location: {jd.get('location', 'DACH')}

PITCH LOGIC — this is critical:
- The posted role is: {role_info.get('engagement_type', 'Unknown')}
- If the role is Full-time: position Aline's fractional/interim executive as a BRIDGE solution. The angle is: "While you search for the permanent hire, we can place a fractional executive who has done this before — starts Monday, owns the function from day one, de-risks the transition."
- If the role is Fractional or Interim: pitch directly. We are the perfect fit. No bridge framing needed.
- Always translate the role into Aline's language: we place operators, not candidates.
- IMPORTANT: Match the executive type to the DECISION MAKER who would hire for this role, not the role function itself. Examples:
  - Data Science / Analytics role → the buyer is the CTO or VP Engineering → pitch a fractional CTO
  - Sales role → the buyer is the CEO or CRO → pitch a fractional Sales/Revenue leader
  - Finance role → the buyer is the CEO → pitch a fractional CFO
  Think about who OWNS this hire and what Aline executive type maps to that buyer's need.

STRUCTURE (follow this order):
1. Greeting: "Hi [First Name]," — ALWAYS use the decision maker's first name. Never skip it.
2. First line: reference the specific signal and company context (use enrichment data). Write complete sentences with proper grammar.
3. Bridge/pitch: why a fractional/interim executive makes sense for THIS situation.
4. Proof/social proof: one line about Aline's credibility. Our partners have operated at Microsoft, Deutsche Bank, Oda, and Zalando. Pick the reference that is most relevant to the target company's industry or stage. Do NOT list all four — pick one or two that resonate.
5. CTA: include the booking link https://cal.com/niels-zanotto/30min for scheduling.
6. Sign-off: "Best, Niels" — always end with this.

Rules:
- Max 5 sentences in the body (excluding greeting and sign-off). No fluff.
- Write complete, professional sentences. Never drop the subject ("I", "We"). Not sloppy-casual.
- No "I hope this finds you well". No "Dear Sir/Madam".
- Subject line: max 8 words, no clickbait.
- Language: English unless company is clearly German-only (check domain/name).

Return JSON:
{{
  "subject": "...",
  "body": "...",
  "pitch_type": "bridge" or "direct",
  "reasoning": "why this angle for this signal type"
}}"""}],
        )
        for block in response.content:
            if hasattr(block, "text"):
                text = block.text.strip()
                start = text.find("{")
                end = text.rfind("}")
                if start != -1 and end != -1:
                    email_data = json.loads(text[start:end+1])
                    p("\U0001f4e7", f"Subject: {email_data.get('subject', '')}")
                    print()
                    p("\U0001f4dd", "Body:")
                    print(email_data.get("body", ""))
                    print()
                    p("\U0001f4a1", f"Reasoning: {email_data.get('reasoning', '')}")
                    return email_data
    except Exception as e:
        p("\u274c", f"Email generation failed: {e}")

    return {}


# --- Step 6: Summary ---

def print_summary(jd: dict, dm: dict, email_data: dict):
    """Print what would have happened in live mode."""
    print()
    print("=" * 60)
    p("\U0001f6ab", "DRY RUN — nothing was sent or written.")
    print()
    if dm.get("email"):
        p("  ", f"Would have: added {dm.get('name', dm['email'])} to Instantly campaign")
    else:
        p("  ", f"Would have: searched for email for {dm.get('name', 'unknown')}")
    p("  ", "Would have: updated Attio role → sdr_contacted")
    p("  ", "Would have: sent Slack alert to #aline-hot-leads")
    if email_data.get("subject"):
        p("  ", f"Would have: sent email with subject \"{email_data['subject']}\"")
    print("=" * 60)


# --- Main ---

def main():
    global VERBOSE, SKIP_TAVILY

    parser = argparse.ArgumentParser(description="Aline Dry Run — test the full outreach pipeline")
    parser.add_argument("--url", required=True, help="JD URL to test")
    parser.add_argument("--verbose", action="store_true", help="Print full API responses")
    parser.add_argument("--no-apollo", action="store_true", help="Skip Apollo search")
    parser.add_argument("--no-tavily", action="store_true", help="Skip Tavily calls (avoids SSL issues on macOS Python 3.9)")
    args = parser.parse_args()

    VERBOSE = args.verbose
    SKIP_TAVILY = args.no_tavily

    global claude
    if not ANTHROPIC_API_KEY:
        p("❌", "ANTHROPIC_API_KEY not set. Export it first.")
        sys.exit(1)
    claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    if not SKIP_TAVILY and not TAVILY_API_KEY:
        p("⚠️", "TAVILY_API_KEY not set. Use --no-tavily to skip Tavily calls.")
        sys.exit(1)

    print()
    print("=" * 60)
    print("  ALINE DRY RUN — Full Pipeline Test")
    print("=" * 60)
    print()

    # Step 1: Fetch JD
    jd = fetch_jd(args.url)
    if not jd.get("title") and not jd.get("description"):
        p("\u274c", "Could not extract JD content. Aborting.")
        sys.exit(1)
    print()

    # Step 2: Enrich company
    company_info = {}
    if jd.get("company"):
        company_info = enrich_company(jd["company"])
    else:
        p("\u26a0\ufe0f", "No company name found in JD, skipping enrichment")
    print()

    # Step 3: Classify role
    role_info = classify_role(jd)
    print()

    # Step 4: Find decision maker
    domain = company_info.get("domain", "")
    dm = find_decision_maker(
        company_name=jd.get("company", ""),
        domain=domain,
        role_info=role_info,
        skip_apollo=args.no_apollo,
    )
    print()

    # Step 5: Generate email
    email_data = generate_email(jd, company_info, role_info, dm)
    print()

    # Step 6: Summary
    print_summary(jd, dm, email_data)


if __name__ == "__main__":
    main()
