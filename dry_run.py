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
PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")
ATTIO_API_KEY = os.environ.get("ATTIO_API_KEY", "")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

claude = None  # initialized in main()

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
})

VERBOSE = False
SKIP_TAVILY = False


def perplexity_ask(query: str) -> str:
    """Ask Perplexity Sonar a question, return the text answer."""
    if not PERPLEXITY_API_KEY:
        return ""
    try:
        resp = SESSION.post(
            "https://api.perplexity.ai/chat/completions",
            headers={"Authorization": f"Bearer {PERPLEXITY_API_KEY}"},
            json={
                "model": "sonar",
                "messages": [{"role": "user", "content": query}],
            },
            timeout=20,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        p("⚠️", f"Perplexity failed: {e}")
        return ""

# --- Helpers ---

def p(emoji: str, msg: str, quiet: bool = False):
    """Print with emoji prefix. If quiet=True, only print in verbose mode."""
    if quiet and not VERBOSE:
        return
    print(f"{emoji} {msg}")


def p_verbose(label: str, data):
    """Print only in verbose mode."""
    if VERBOSE:
        print(f"  [DEBUG] {label}: {json.dumps(data, indent=2, default=str)[:2000]}")


# --- Step 1: Fetch JD ---

def fetch_jd(url: str) -> dict:
    """Fetch and parse a job description from URL."""
    p("  ", f"Fetching JD from: {url}", quiet=True)

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
    company_url = ""
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
                company_url = (org.get("sameAs") or org.get("url") or "") if isinstance(org, dict) else ""
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

    p("✓", f"JD: {title or '(no title)'} at {company or '(no company)'}")
    if location:
        p("  ", f"Location: {location}", quiet=True)

    result = {
        "title": title,
        "company": company,
        "company_url": company_url,
        "location": location,
        "description": description,
        "url": url,
    }
    p_verbose("JD data", {k: v[:200] if isinstance(v, str) else v for k, v in result.items()})
    return result


# --- Step 2: Enrich company ---

def enrich_company(company_name: str, company_url: str = "") -> dict:
    """Enrich company info via Perplexity (primary) or Tavily (fallback)."""
    p("  ", f"Enriching {company_name}...", quiet=True)

    # Extract domain from company URL if available
    known_domain = ""
    if company_url:
        from urllib.parse import urlparse
        parsed = urlparse(company_url)
        known_domain = parsed.netloc or parsed.path
        known_domain = known_domain.removeprefix("www.")

    # Try Perplexity first
    perplexity_context = ""
    if PERPLEXITY_API_KEY:
        p("  ", "Using Perplexity for enrichment...", quiet=True)
        perplexity_context = perplexity_ask(
            f"What does {company_name} do? Include: company website/domain, "
            f"approximate number of employees, funding stage (e.g. Series A/B/C, "
            f"bootstrapped), and a one-line description of what the company does. "
            f"Keep it brief and factual."
        )
        p_verbose("Perplexity company result", perplexity_context)

    # Tavily fallback if no Perplexity
    tavily_context = ""
    if not perplexity_context and not SKIP_TAVILY and TAVILY_API_KEY:
        from tavily import TavilyClient
        tavily = TavilyClient(api_key=TAVILY_API_KEY)
        try:
            resp = tavily.search(
                query=f"{company_name} company domain website funding employees DACH",
                max_results=3, days=30,
            )
            results = resp.get("results", [])
            p_verbose("Tavily company results", results)
            tavily_context = "\n".join([
                f"- {r.get('title', '')}: {r.get('content', '')[:300]}"
                for r in results
            ])
        except Exception as e:
            p("⚠️", f"Tavily search failed: {e}")

    context = perplexity_context or tavily_context or ""

    try:
        response = claude.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=512,
            system="Extract company info. Return only valid JSON.",
            messages=[{"role": "user", "content": f"""From this research about "{company_name}", extract:
- domain (company website domain, e.g. acme.de)
- headcount_estimate (number or "unknown")
- funding_stage (e.g. "Series A", "Series B", "Bootstrapped", "unknown")
- one_liner (what the company does, max 10 words)

Research:
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
                    # Prefer domain from JD structured data over Claude's guess
                    if known_domain:
                        info["domain"] = known_domain
                    p("✓", f"Company: {company_name} — {info.get('one_liner', '?')} ({info.get('funding_stage', '?')}, ~{info.get('headcount', '?')} people)")
                    p("  ", f"Domain: {info.get('domain', '?')}", quiet=True)
                    return info
    except Exception as e:
        p("\u26a0\ufe0f", f"Claude enrichment failed: {e}")

    return {"domain": known_domain or "", "headcount": "unknown", "funding_stage": "unknown"}


# --- Step 3: Classify role ---

def classify_role(jd: dict) -> dict:
    """Use Claude to classify the role."""
    p("  ", "Classifying role...", quiet=True)

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
  "uses_agency": true if the JD mentions contacting a recruiter/agency to apply (e.g. "talk to Alex", "contact our recruitment partner", "apply through [agency name]") — false otherwise,
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
                    p("✓", f"Role: {info.get('engagement_type', '?')} {info.get('role_function', '?')} — {info.get('signal_type', '?')}")
                    if info.get("reasoning"):
                        p("  ", f"Reasoning: {info['reasoning']}", quiet=True)
                    return info
    except Exception as e:
        p("\u274c", f"Classification failed: {e}")

    return {"engagement_type": "Unknown", "role_function": "Unknown", "signal_type": "Unknown"}


# --- Step 4: Find decision maker ---

def find_decision_maker(company_name: str, domain: str, role_info: dict,
                        skip_apollo: bool = False) -> dict:
    """Find the decision maker via Apollo and/or Tavily."""
    p("  ", "Finding decision maker...", quiet=True)

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
                    p("  ", f"Target: {targeting.get('target_titles', [])} — {targeting.get('reasoning', '')}", quiet=True)
                    break
        else:
            targeting = {"target_titles": ["CEO", "Founder"]}
    except Exception as e:
        p("\u26a0\ufe0f", f"Reasoning failed: {e}")
        targeting = {"target_titles": ["CEO", "Founder"]}

    # Apollo search
    if not skip_apollo and APOLLO_API_KEY and domain:
        p("  ", f"Apollo search: domain={domain}, titles={targeting['target_titles']}", quiet=True)
        try:
            resp = SESSION.post(
                "https://api.apollo.io/v1/mixed_people/search",
                headers={"X-Api-Key": APOLLO_API_KEY},
                json={
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
                    p("✓", f"DM: {name}, {title} — {email}")
                else:
                    p("✓", f"DM: {name}, {title} (no email, LinkedIn: {linkedin})")

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
        p("  ", "Skipping Apollo (--no-apollo flag)", quiet=True)

    # Perplexity search (primary)
    if PERPLEXITY_API_KEY:
        target_title = targeting["target_titles"][0]
        p("  ", f"Perplexity: searching for {target_title} at {company_name}", quiet=True)
        pplx_result = perplexity_ask(
            f"Who is the {target_title} of {company_name}? "
            f"I need their full name, exact job title, and LinkedIn profile URL. "
            f"Only return someone who currently works at {company_name}. "
            f"Keep the answer brief and factual."
        )
        p_verbose("Perplexity DM result", pplx_result)

        if pplx_result:
            extract_resp = claude.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=256,
                system="Extract person info. Return only valid JSON.",
                messages=[{"role": "user", "content": f"""From this research, extract the {target_title} of {company_name}.

CRITICAL: The person MUST actually work at {company_name}. Do NOT return people who work at other companies.

Research:
{pplx_result}

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
                            p("✓", f"DM: {person['name']}, {person.get('title', '')} (via Perplexity)")
                            person["source"] = "Perplexity"
                            return person

    # Tavily fallback
    if SKIP_TAVILY or not TAVILY_API_KEY:
        p("✓", f"DM: {targeting['target_titles'][0]} (placeholder)")
        return {
            "name": f"[{targeting['target_titles'][0]}]",
            "title": targeting["target_titles"][0],
            "email": "",
            "linkedin_url": "",
            "source": "placeholder",
        }

    p("  ", f"Tavily fallback: searching for {targeting['target_titles'][0]} at {company_name}", quiet=True)
    from tavily import TavilyClient
    tavily = TavilyClient(api_key=TAVILY_API_KEY)

    try:
        resp = tavily.search(
            query=f'"{company_name}" {targeting["target_titles"][0]} site:linkedin.com/in',
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
                messages=[{"role": "user", "content": f"""From these search results, extract the {targeting['target_titles'][0]} of {company_name}.

CRITICAL: The person MUST actually work at {company_name}. Do NOT return people who work at other companies. If none of the results contain someone who works at {company_name}, return empty fields.

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
                            p("✓", f"DM: {person['name']}, {person.get('title', '')} (via Tavily)")
                            if not person.get("email"):
                                p("  ", "No email found — LinkedIn only.", quiet=True)
                            person["source"] = "Tavily"
                            return person
    except Exception as e:
        p("\u26a0\ufe0f", f"Tavily people search failed: {e}")

    p("\u274c", "No decision maker found via any source")
    return {"name": "", "title": "", "email": "", "linkedin_url": "", "source": "none"}


# --- Step 5: Generate email ---

def generate_email(jd: dict, company_info: dict, role_info: dict, dm: dict) -> dict:
    """Generate outreach email — short, human, no brand speak."""
    p("  ", "Generating email...", quiet=True)

    first_name = dm.get("name", "").split()[0] if dm.get("name") else "there"
    engagement = role_info.get("engagement_type", "Unknown")
    one_liner = company_info.get("one_liner", "")
    company = jd.get("company", "")
    title = jd.get("title", "")

    # Build pitch hint based on engagement type
    if engagement in ("Fractional", "Interim"):
        pitch_hint = f"This is a {engagement.lower()} role — pitch directly. We do exactly this."
        pitch_type = "direct"
    else:
        pitch_hint = f"This is a full-time role. Offer a fractional/interim {title} as a bridge while they search for the permanent hire."
        pitch_type = "bridge"

    try:
        response = claude.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1024,
            system="You are Niels writing a quick outreach email. Write like a normal person — not a copywriter, not a brand strategist. Return only valid JSON.",
            messages=[{"role": "user", "content": f"""Write an outreach email from Niels (founder of Aline) to {first_name}.

Context:
- Role: {title} at {company}{f" — {one_liner}" if one_liner else ""}
- {pitch_hint}
- Match the role level — never pitch up (e.g. don't offer a CTO for an engineer role)

Here are 3 example emails. Vary your structure — don't copy one template:

EXAMPLE A (fractional role, shortest):
Hi Hannes, I came across the fractional legal counsel role. I think we have a few candidates who could be a good fit. We're Aline — an AI-driven search firm built by former Zalando and Deutsche Bank talent execs. We do interim, fractional, and executive hiring for scaling teams. Should we hop on a call this week? https://cal.com/niels-zanotto/30min Best, Niels

EXAMPLE B (full-time role, mentions company):
Hi Sarah, saw the Head of Engineering opening at Cometa. We're Aline — an AI-driven search firm founded by former Microsoft, Zalando, and Oda talent execs. We work with DACH tech teams on senior hires like this. I think we have some interesting profiles worth showing you. Happy to share them if useful — here's my calendar: https://cal.com/niels-zanotto/30min Best, Niels

EXAMPLE C (short and direct):
Hi Tom, the VP Sales role caught my eye. We're Aline — an AI-driven search firm built by former Zalando, Deutsche Bank, and Microsoft talent execs. We help scaling companies fill exactly these kinds of roles. Want to jump on a quick call? https://cal.com/niels-zanotto/30min Best, Niels

HARD RULES — break any of these and the email is useless:
1. Write like a human. No pseudo-analysis, no insight theater, no brand speak.
2. Always introduce Aline by name: "We're Aline — former [companies] talent execs." Never skip the name.
3. "We" not "my team". Only claim where the team COMES FROM (Zalando, Deutsche Bank, Microsoft, Oda) — never what they "led" or "placed".
4. Mention what the company does only if it fits naturally in one clause. Don't rephrase the JD.
5. Match the role level — never pitch up.
6. Under 100 words. End with casual CTA + https://cal.com/niels-zanotto/30min + "Best, Niels"

Subject line: max 6 words, lowercase feel, no clickbait.

Return JSON:
{{
  "subject": "...",
  "body": "...",
  "pitch_type": "{pitch_type}",
  "reasoning": "one sentence on the angle"
}}"""}],
        )
        for block in response.content:
            if hasattr(block, "text"):
                text = block.text.strip()
                start = text.find("{")
                end = text.rfind("}")
                if start != -1 and end != -1:
                    email_data = json.loads(text[start:end+1])
                    return email_data
    except Exception as e:
        p("\u274c", f"Email generation failed: {e}")

    return {}


# --- Step 6: Summary ---

def print_summary(jd: dict, dm: dict, email_data: dict):
    """Print the final output: email + metadata."""
    print()
    print("=" * 60)
    if email_data.get("subject"):
        print(f"  Subject: {email_data['subject']}")
        print("=" * 60)
        print()
        print(email_data.get("body", ""))
        print()
        print("-" * 60)
        print(f"  Pitch: {email_data.get('pitch_type', '?')} | {email_data.get('reasoning', '')}")
        print(f"  To: {dm.get('name', '?')} ({dm.get('title', '?')}) — {dm.get('email', 'no email')}")
        print(f"  Role: {jd.get('title', '?')} at {jd.get('company', '?')}")
        print("-" * 60)
        print("  DRY RUN — nothing was sent.")
    else:
        print("  No email generated.")
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

    # Step 1: Fetch JD
    jd = fetch_jd(args.url)
    if not jd.get("title") and not jd.get("description"):
        p("✗", "Could not extract JD content. Aborting.")
        sys.exit(1)

    # Step 2: Enrich company
    company_info = {}
    if jd.get("company"):
        company_info = enrich_company(jd["company"], jd.get("company_url", ""))
    else:
        p("⚠️", "No company name found in JD, skipping enrichment")

    # Step 3: Classify role
    role_info = classify_role(jd)

    # Step 4: Find decision maker
    domain = company_info.get("domain", "")
    dm = find_decision_maker(
        company_name=jd.get("company", ""),
        domain=domain,
        role_info=role_info,
        skip_apollo=args.no_apollo,
    )

    # Step 5: Generate email
    email_data = generate_email(jd, company_info, role_info, dm)

    # Step 6: Output
    print_summary(jd, dm, email_data)


if __name__ == "__main__":
    main()
