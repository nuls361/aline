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
    """Enrich company info via Tavily."""
    p("  ", f"Enriching {company_name}...", quiet=True)

    # Extract domain from company URL if available
    known_domain = ""
    if company_url:
        from urllib.parse import urlparse
        parsed = urlparse(company_url)
        known_domain = parsed.netloc or parsed.path
        known_domain = known_domain.removeprefix("www.")

    results = []
    if SKIP_TAVILY:
        p("  ", "Skipping Tavily — Claude-only enrichment", quiet=True)
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

    # Fallback: Tavily search
    if SKIP_TAVILY:
        p("✓", f"DM: {targeting['target_titles'][0]} (placeholder — no Tavily)")
        return {
            "name": f"[{targeting['target_titles'][0]}]",
            "title": targeting["target_titles"][0],
            "email": "",
            "linkedin_url": "",
            "source": "placeholder (--no-tavily)",
        }

    p("  ", f"Tavily fallback: searching for {targeting['target_titles'][0]} at {company_name}", quiet=True)
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
    """Generate outreach email using Claude + soul.md + skill.md."""
    p("  ", "Generating email...", quiet=True)

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

JOB DESCRIPTION (use specific details from this for the context hook):
{jd.get('description', '')[:2000]}

WHO WE ARE (weave this in naturally, don't copy-paste):
Aline is a team of former talent executives from companies like Zalando, Microsoft, Deutsche Bank, and Oda. We do interim, fractional, and executive hiring for companies that are scaling.

PITCH LOGIC:
- The posted role is: {role_info.get('engagement_type', 'Unknown')}
- If Full-time: mention we have candidates who could fit, and suggest a call to discuss. Don't go deep on the fractional model in the email — save that for the meeting.
- If Fractional or Interim: pitch directly. We are a perfect fit. Mention we have relevant candidates.
- Match the offer to the ROLE being hired. If they hire a Senior Engineer, talk about senior engineers. Never jump to CTO/VP Eng. Never pitch up or down.
- AGENCY CHECK: {role_info.get('uses_agency', False)}. If the JD mentions applying through an agency or recruiter, acknowledge it — e.g. "I noticed you're working with a recruiter on this. We sometimes complement that with [our angle]." Don't ignore it.

HOW TO WRITE THE EMAIL:
Write like a real person sending a quick email to someone they respect. NOT a template. NOT a sales sequence. Think: you're a talent exec who found an interesting role and wants to reach out casually.

Good example (use the VIBE, not the exact words):
"Hi Hannes, I came across the fractional legal counsel role — sounds like an interesting setup. I think we have a handful of candidates who could be a good fit. We're Aline, a bunch of former Zalando and Deutsche Bank talent execs. We do interim, fractional and executive hiring for companies that are scaling. Should we jump on a call in the next few days to talk about the candidates?"

What makes this good:
- Casual, human tone
- Shows you know the role (brief reference)
- Introduces Aline naturally (who we are, not what we sell)
- CTA is a simple "should we chat" — not a diagnostic question
- No promises about what a candidate would "own" or "build"
- No rigid structure — reads like a real email

BAD patterns to avoid:
- "We're Aline — we place fractional and interim executives into DACH tech teams." (too salesy)
- "They'd own pipeline, messaging, and first revenue while you find the right long-term fit." (too specific, presumptuous)
- "Our partners have led GTM at Oda, Zalando, and similar product-led teams." (fake claim)
- "building interactive interfaces for billion-document repositories" (no human writes like this)
- "Do you have legal capacity in place right now?" (diagnostic question — not your business)
- Any opener starting with "I saw you're hiring X"

Rules:
- 4–6 sentences. ~60–100 words. Keep it SHORT.
- Reference something specific about the COMPANY or ROLE so they know you did your homework — but keep it natural, one clause, not a paragraph.
- Introduce Aline as who we are (former talent execs from [companies]), not what we sell.
- Social proof = where our TEAM comes from, not where our "partners have led [function]." We are former talent execs from these companies. That's it.
- CTA: "Should we jump on a call?" or "Happy to share profiles if useful." Then: "https://cal.com/niels-zanotto/30min". End with "Best, Niels".
- Tone: casual, peer-to-peer, confident but not pushy. Like you're texting a professional contact.
- Subject line: max 6 words. Casual. Reference the role or company.
- Language: English unless company is clearly German-only.
- Every email should feel DIFFERENT. Vary sentence structure, length, and order. No two emails should read the same.

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
