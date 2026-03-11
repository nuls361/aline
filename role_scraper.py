"""
Aline Role Scraper — JSearch + Wellfound → Attio CRM

Scrapes interim/fractional/C-level executive roles in DACH from:
1. JSearch API (RapidAPI) — broad job board aggregator
2. Wellfound (ex-AngelList) — startup executive roles

Classifies roles by engagement_type and role_function, then writes
to Attio CRM (company upsert + role creation) and alerts Slack.
"""

import os
import json
import re
import time
import logging
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from attio_client import attio

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- Config ---
RAPIDAPI_KEY = os.environ["RAPIDAPI_KEY"]
SLACK_WEBHOOK = os.environ["SLACK_WEBHOOK_HOT_LEADS"]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SENT_URLS_PATH = os.path.join(BASE_DIR, "role_scraper_sent_urls.json")

SESSION = requests.Session()

# --- JSearch ---

JSEARCH_QUERIES = [
    # Interim roles
    "Interim CFO Germany", "Interim CTO Germany", "Interim COO Germany",
    "Interim CEO Germany", "Interim CHRO Germany", "Interim CMO Germany",
    "Interim CFO Austria", "Interim CFO Switzerland",
    # Fractional roles
    "Fractional CFO Germany", "Fractional CTO Germany", "Fractional COO Germany",
    "Fractional CFO DACH",
    # C-level
    "Chief Financial Officer startup Berlin", "Chief Technology Officer startup Munich",
    "Chief Operating Officer startup Germany",
    "Chief People Officer Germany", "Chief Revenue Officer Germany",
    # VP / Head of
    "VP Finance Germany startup", "VP Engineering Berlin",
    "Head of Finance Germany startup", "Head of Engineering Berlin",
    "Head of People Germany startup", "Head of Product Munich",
    # Managing Director
    "Managing Director Germany startup", "Geschäftsführer startup Berlin",
    # Austria + Switzerland
    "Interim CTO Austria", "Interim CFO Zurich",
    "Chief Financial Officer Vienna startup", "Head of Engineering Zurich",
]

JSEARCH_HEADERS = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
}


def fetch_jsearch(query: str) -> list[dict]:
    """Fetch jobs from JSearch API for a single query."""
    url = "https://jsearch.p.rapidapi.com/search"
    params = {
        "query": query,
        "page": "1",
        "num_pages": "1",
        "date_posted": "week",
        "country": "de,at,ch",
    }
    try:
        resp = SESSION.get(url, headers=JSEARCH_HEADERS, params=params, timeout=10)
        if resp.status_code == 429:
            log.warning("JSearch rate limited, sleeping 10s")
            time.sleep(10)
            return []
        resp.raise_for_status()
        data = resp.json()
        results = []
        for job in data.get("data", []):
            results.append({
                "title": job.get("job_title", ""),
                "company": job.get("employer_name", ""),
                "location": f"{job.get('job_city', '')}, {job.get('job_country', '')}".strip(", "),
                "url": job.get("job_apply_link", "") or job.get("job_google_link", ""),
                "posted_date": (job.get("job_posted_at_datetime_utc") or "")[:10] or None,
                "description": (job.get("job_description") or "")[:2000],
                "source": "JSearch",
                "employer_website": job.get("employer_website", ""),
                "is_remote": job.get("job_is_remote", False),
            })
        return results
    except Exception as e:
        log.error(f"JSearch error for '{query}': {e}")
        return []


def run_jsearch() -> list[dict]:
    """Run all JSearch queries with rate limiting."""
    all_jobs = []
    for i, query in enumerate(JSEARCH_QUERIES):
        log.info(f"JSearch [{i+1}/{len(JSEARCH_QUERIES)}]: {query}")
        jobs = fetch_jsearch(query)
        all_jobs.extend(jobs)
        if i < len(JSEARCH_QUERIES) - 1:
            time.sleep(3)
    log.info(f"JSearch total: {len(all_jobs)} jobs from {len(JSEARCH_QUERIES)} queries")
    return all_jobs


# --- Wellfound ---

WELLFOUND_SEARCHES = [
    # Germany
    {"url": "https://wellfound.com/role/l/cfo/germany", "role_hint": "CFO"},
    {"url": "https://wellfound.com/role/l/cto/germany", "role_hint": "CTO"},
    {"url": "https://wellfound.com/role/l/coo/germany", "role_hint": "COO"},
    {"url": "https://wellfound.com/role/l/head-of-finance/germany", "role_hint": "Head of Finance"},
    {"url": "https://wellfound.com/role/l/head-of-engineering/germany", "role_hint": "Head of Engineering"},
    {"url": "https://wellfound.com/role/l/head-of-people/germany", "role_hint": "Head of People"},
    {"url": "https://wellfound.com/role/l/vp-of-finance/germany", "role_hint": "VP Finance"},
    {"url": "https://wellfound.com/role/l/head-of-product/germany", "role_hint": "Head of Product"},
    # Austria
    {"url": "https://wellfound.com/role/l/cfo/austria", "role_hint": "CFO"},
    {"url": "https://wellfound.com/role/l/cto/austria", "role_hint": "CTO"},
    {"url": "https://wellfound.com/role/l/head-of-finance/austria", "role_hint": "Head of Finance"},
    # Switzerland
    {"url": "https://wellfound.com/role/l/cfo/switzerland", "role_hint": "CFO"},
    {"url": "https://wellfound.com/role/l/cto/switzerland", "role_hint": "CTO"},
    {"url": "https://wellfound.com/role/l/head-of-finance/switzerland", "role_hint": "Head of Finance"},
]

WELLFOUND_SESSION = requests.Session()
WELLFOUND_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
})


def scrape_wellfound_page(url: str, role_hint: str) -> list[dict]:
    """Scrape a single Wellfound search results page."""
    jobs = []
    try:
        resp = WELLFOUND_SESSION.get(url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Method 1: __NEXT_DATA__
        next_data_script = soup.find("script", {"id": "__NEXT_DATA__"})
        if next_data_script:
            try:
                data = json.loads(next_data_script.string)
                props = data.get("props", {}).get("pageProps", {})
                job_listings = (
                    props.get("jobListings", []) or
                    props.get("jobs", []) or
                    props.get("results", []) or
                    []
                )
                for listing in job_listings:
                    startup = listing.get("startup", listing.get("company", {})) or {}
                    company_name = startup.get("name", "")
                    if not company_name:
                        continue
                    slug = listing.get("slug", "")
                    jobs.append({
                        "title": listing.get("title", listing.get("role", role_hint)),
                        "company": company_name,
                        "location": listing.get("location", listing.get("locationNames", "")),
                        "url": f"https://wellfound.com/jobs/{slug}" if slug else url,
                        "posted_date": None,
                        "description": (listing.get("description") or "")[:2000],
                        "source": "Wellfound",
                        "employer_website": startup.get("website_url", ""),
                        "is_remote": listing.get("remote", False),
                    })
                if jobs:
                    return jobs
            except (json.JSONDecodeError, KeyError):
                pass

        # Method 2: JSON-LD
        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                ld_data = json.loads(script.string)
                items = [ld_data] if isinstance(ld_data, dict) else ld_data if isinstance(ld_data, list) else []
                for item in items:
                    if isinstance(item, dict) and item.get("@type") == "JobPosting":
                        org = item.get("hiringOrganization", {}) or {}
                        location = item.get("jobLocation", {})
                        if isinstance(location, list) and location:
                            location = location[0]
                        address = location.get("address", {}) if isinstance(location, dict) else {}
                        jobs.append({
                            "title": item.get("title", role_hint),
                            "company": org.get("name", ""),
                            "location": f"{address.get('addressLocality', '')}, {address.get('addressCountry', '')}".strip(", "),
                            "url": item.get("url", url),
                            "posted_date": (item.get("datePosted") or "")[:10] or None,
                            "description": (item.get("description") or "")[:2000],
                            "source": "Wellfound",
                            "employer_website": org.get("url", ""),
                            "is_remote": "TELECOMMUTE" in str(item.get("jobLocationType", "")),
                        })
            except (json.JSONDecodeError, KeyError):
                continue

        # Method 3: HTML cards
        if not jobs:
            job_cards = soup.select("[data-test='JobCard'], .styles_component__card, div[class*='JobCard']")
            if not job_cards:
                job_cards = soup.find_all("div", class_=re.compile(r"job|listing|card", re.I))
            for card in job_cards:
                title_el = card.find(["h2", "h3", "h4"])
                title = title_el.get_text(strip=True) if title_el else role_hint
                company_el = card.find("a", class_=re.compile(r"company|startup", re.I))
                company = company_el.get_text(strip=True) if company_el else ""
                if not company:
                    continue
                link_el = card.find("a", href=True)
                href = link_el["href"] if link_el else ""
                link = href if href.startswith("http") else f"https://wellfound.com{href}" if href else url
                jobs.append({
                    "title": title,
                    "company": company,
                    "location": "",
                    "url": link,
                    "posted_date": None,
                    "description": card.get_text(" ", strip=True)[:2000],
                    "source": "Wellfound",
                    "employer_website": "",
                    "is_remote": bool(re.search(r"remote", card.get_text(), re.I)),
                })

    except requests.exceptions.RequestException as e:
        log.error(f"Wellfound scrape error for {url}: {e}")

    return jobs


def run_wellfound() -> list[dict]:
    """Run all Wellfound searches."""
    all_jobs = []
    for i, search in enumerate(WELLFOUND_SEARCHES):
        log.info(f"Wellfound [{i+1}/{len(WELLFOUND_SEARCHES)}]: {search['role_hint']} ({search['url']})")
        jobs = scrape_wellfound_page(search["url"], search["role_hint"])
        all_jobs.extend(jobs)
        if i < len(WELLFOUND_SEARCHES) - 1:
            time.sleep(2)
    log.info(f"Wellfound total: {len(all_jobs)} jobs from {len(WELLFOUND_SEARCHES)} searches")
    return all_jobs


# --- Classification ---

ENGAGEMENT_PATTERNS = {
    "Interim": [r"\binterim\b", r"\btemporary\b", r"\bÜbergangs"],
    "Fractional": [r"\bfractional\b", r"\bpart[- ]?time\s+c[- ]?level", r"\bfraktional"],
}

ROLE_FUNCTION_PATTERNS = {
    "Finance": [r"\bcfo\b", r"\bfinance\b", r"\bfinancial\b", r"\bcontrolling\b", r"\btreasur"],
    "Technology": [r"\bcto\b", r"\btechnolog\b", r"\bengineering\b", r"\btech lead\b"],
    "Operations": [r"\bcoo\b", r"\boperation\b", r"\bsupply chain\b", r"\blogistic"],
    "People": [r"\bchro\b", r"\bpeople\b", r"\bhuman resource\b", r"\bhr\b", r"\btalent\b"],
    "Product": [r"\bcpo\b", r"\bproduct\b"],
    "Marketing": [r"\bcmo\b", r"\bmarketing\b", r"\bgrowth\b"],
    "Sales": [r"\bcro\b", r"\bsales\b", r"\brevenue\b", r"\bcommercial\b"],
    "General Management": [r"\bceo\b", r"\bmanaging director\b", r"\bgeschäftsführer\b", r"\bgeneral manager\b"],
}


def classify_engagement(title: str, description: str) -> str:
    text = (title + " " + description).lower()
    for eng_type, patterns in ENGAGEMENT_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, text, re.I):
                return eng_type
    return "Full-time"


def classify_function(title: str) -> str:
    lower = title.lower()
    for func, patterns in ROLE_FUNCTION_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, lower):
                return func
    return "Other"


# --- Deduplication ---

def load_sent_urls() -> set:
    try:
        with open(SENT_URLS_PATH) as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def save_sent_urls(urls: set):
    with open(SENT_URLS_PATH, "w") as f:
        json.dump(sorted(urls), f, indent=2)


def commit_sent_urls():
    try:
        os.system('git config user.email "agent@get-aline.com"')
        os.system('git config user.name "Aline Role Scraper"')
        os.system("git add role_scraper_sent_urls.json")
        os.system('git commit -m "chore: update role_scraper_sent_urls"')
        os.system("git push")
    except Exception as e:
        log.error(f"Git push error: {e}")


# --- Slack ---

def send_slack(text: str):
    try:
        resp = requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"Slack error: {e}")


def format_finding(role: dict) -> str:
    eng = role.get("engagement_type", "")
    emoji = "\U0001f525" if eng in ("Interim", "Fractional") else "\U0001f3af"
    return (
        f"{emoji} *{role['title']} — {role['company']} ({role['source']})*\n"
        f"Type: {eng} | Function: {role.get('role_function', '')}\n"
        f"Location: {role.get('location') or 'Not specified'}\n"
        f"<{role['url']}|View role>"
    )


def format_summary(jsearch_count: int, wellfound_count: int, written: int, alerted: int) -> str:
    now = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M")
    return (
        f"\u2705 *Role Scraper — {now}*\n"
        f"JSearch: {jsearch_count} | Wellfound: {wellfound_count} | "
        f"Written to Attio: {written} | Slack alerts: {alerted}"
    )


# --- Attio writes ---

def extract_domain(url: str) -> str:
    """Extract domain from a URL."""
    if not url:
        return ""
    url = url.lower().strip()
    for prefix in ["https://", "http://", "www."]:
        if url.startswith(prefix):
            url = url[len(prefix):]
    return url.split("/")[0].split("?")[0]


def write_to_attio(role: dict) -> bool:
    """Upsert company and create role in Attio. Returns True on success."""
    domain = extract_domain(role.get("employer_website", ""))
    if not domain:
        log.debug(f"No domain for {role.get('company', '?')}, skipping Attio write")
        return False

    try:
        # Upsert company
        company_resp = attio.upsert_company(domain=domain, values={
            "name": [{"value": role["company"]}],
        })
        if not company_resp:
            log.warning(f"Failed to upsert company {role['company']}")
            return False

        company_id = attio.extract_record_id(company_resp.get("data", company_resp))
        if not company_id:
            log.warning(f"No company_id for {role['company']}")
            return False

        # Create role
        role_values = {
            "name": [{"value": role["title"]}],
            "company": attio.format_record_reference("companies", company_id),
            "source": attio.format_select(role["source"]),
            "source_url": [{"value": role["url"]}],
            "engagement_type": attio.format_select(role.get("engagement_type", "Full-time")),
            "role_function": attio.format_select(role.get("role_function", "Other")),
            "location": [{"value": role.get("location", "")}],
        }
        if role.get("posted_date"):
            role_values["posted_date"] = [{"value": role["posted_date"]}]

        resp = attio.create_role(role_values)
        if resp:
            log.info(f"  Attio: created role '{role['title']}' at {role['company']}")
            return True
        else:
            log.warning(f"  Attio: failed to create role '{role['title']}' at {role['company']}")
            return False

    except Exception as e:
        log.error(f"Attio write error for {role.get('company', '?')}: {e}")
        return False


# --- Dedup by URL across both sources ---

def deduplicate(jobs: list[dict]) -> list[dict]:
    """Remove duplicate jobs by URL."""
    seen = set()
    unique = []
    for job in jobs:
        url = job.get("url", "")
        if not url or url in seen:
            continue
        seen.add(url)
        unique.append(job)
    return unique


# --- Main ---

def main():
    sent_urls = load_sent_urls()

    # 1. Scrape JSearch
    jsearch_jobs = run_jsearch()

    # 2. Scrape Wellfound
    wellfound_jobs = run_wellfound()

    # 3. Merge and deduplicate
    all_jobs = deduplicate(jsearch_jobs + wellfound_jobs)
    log.info(f"Total unique jobs after dedup: {len(all_jobs)}")

    # 4. Classify, filter new, write to Attio, alert Slack
    written = 0
    alerted = 0

    for job in all_jobs:
        url = job.get("url", "")
        if url in sent_urls:
            continue

        # Classify
        job["engagement_type"] = classify_engagement(job["title"], job.get("description", ""))
        job["role_function"] = classify_function(job["title"])

        # Write to Attio
        if write_to_attio(job):
            written += 1

        # Slack alert for Interim/Fractional roles
        if job["engagement_type"] in ("Interim", "Fractional"):
            send_slack(format_finding(job))
            alerted += 1

        sent_urls.add(url)

    # 5. Summary
    send_slack(format_summary(len(jsearch_jobs), len(wellfound_jobs), written, alerted))
    save_sent_urls(sent_urls)
    commit_sent_urls()
    log.info(f"Done. Written: {written} | Alerted: {alerted}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"Fatal error: {e}")
        try:
            send_slack(f"\u26a0\ufe0f *Role Scraper Error*\n{e}")
        except Exception:
            pass
        raise
