import os
import json
import logging
import time
from datetime import datetime, timezone

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- Config ---
SLACK_WEBHOOK = os.environ["SLACK_WEBHOOK_HOT_LEADS"]
SENT_URLS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demand_sent_urls.json")

COMPANIES = [
    "personio", "celonis", "forto", "billie", "taxfix", "staffbase", "contentful",
    "mambu", "adjustcom", "homeday", "sennder", "moonfare", "sumup", "tier",
    "picsart", "commercetools", "spryker", "iloq", "babbel", "helpling",
    "clark", "getsafe", "wefox", "ottonova", "nuri", "penta", "kontist",
    "solaris", "raisin", "deposit-solutions", "smava", "zinsbaustein",
    "scalable-capital", "justtrade", "litfinance", "bitpanda-tech",
    "relayr", "contiamo", "oda", "voi-technology", "unu-motors",
    "zenjob", "workmotion", "remote", "factorial", "kenjo", "leapsome",
    "lano", "hibob", "humaans", "pleo"
]

TITLE_KEYWORDS = [
    "head of", "vp ", "vp,", "vice president", "chief", "director",
    "interim", "fractional", "managing director", "general manager", "c-level"
]

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Aline-Demand-Agent/1.0"})

# --- ATS Fetchers ---

def fetch_ashby(slug: str) -> list[dict]:
    """Ashby public job board API."""
    url = f"https://api.ashbyhq.com/posting-public/job-board/{slug}"
    try:
        resp = SESSION.get(url, timeout=10)
        if resp.status_code in (401, 404):
            return []
        resp.raise_for_status()
        data = resp.json()
        results = []
        for job in data.get("jobs", []):
            title = job.get("title", "")
            if not matches_title(title):
                continue
            location = job.get("location", "")
            if isinstance(location, dict):
                location = location.get("name", "")
            results.append({
                "company": slug,
                "title": title,
                "location": location,
                "url": job.get("jobUrl", ""),
                "posted_date": job.get("publishedAt"),
                "ats": "Ashby"
            })
        return results
    except Exception as e:
        log.debug(f"Ashby {slug}: {e}")
        return []


def fetch_greenhouse(slug: str) -> list[dict]:
    """Greenhouse JSON API — returns jobs with title, location, URL."""
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    try:
        resp = SESSION.get(url, timeout=10)
        if resp.status_code in (404, 400):
            return []
        resp.raise_for_status()
        data = resp.json()
        results = []
        for job in data.get("jobs", []):
            title = job.get("title", "")
            if not matches_title(title):
                continue
            location = job.get("location", {}).get("name", "") if isinstance(job.get("location"), dict) else ""
            job_url = job.get("absolute_url", "")
            updated = job.get("updated_at", "")
            posted_date = updated[:10] if updated else None
            results.append({
                "company": slug,
                "title": title,
                "location": location,
                "url": job_url,
                "posted_date": posted_date,
                "ats": "Greenhouse"
            })
        return results
    except Exception as e:
        log.debug(f"Greenhouse {slug}: {e}")
        return []


def fetch_lever(slug: str) -> list[dict]:
    """Lever JSON API — returns postings with title, location, URL."""
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    try:
        resp = SESSION.get(url, timeout=10)
        if resp.status_code in (404, 400):
            return []
        resp.raise_for_status()
        postings = resp.json()
        if not isinstance(postings, list):
            return []
        results = []
        for posting in postings:
            title = posting.get("text", "")
            if not matches_title(title):
                continue
            location = posting.get("categories", {}).get("location", "") if isinstance(posting.get("categories"), dict) else ""
            created_at = posting.get("createdAt")
            posted_date = None
            if created_at:
                try:
                    posted_date = datetime.fromtimestamp(created_at / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    pass
            results.append({
                "company": slug,
                "title": title,
                "location": location,
                "url": posting.get("hostedUrl", ""),
                "posted_date": posted_date,
                "ats": "Lever"
            })
        return results
    except Exception as e:
        log.debug(f"Lever {slug}: {e}")
        return []


def fetch_workable(slug: str) -> list[dict]:
    """Workable public API v3 — POST to get jobs list."""
    url = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs"
    try:
        resp = SESSION.post(url, json={}, timeout=10)
        if resp.status_code in (404, 400):
            return []
        resp.raise_for_status()
        data = resp.json()
        results = []
        for job in data.get("results", []):
            title = job.get("title", "")
            if not matches_title(title):
                continue
            location_parts = []
            if job.get("city"):
                location_parts.append(job["city"])
            if job.get("country"):
                location_parts.append(job["country"])
            location = ", ".join(location_parts)
            shortcode = job.get("shortcode", "")
            job_url = f"https://apply.workable.com/{slug}/j/{shortcode}/" if shortcode else ""
            results.append({
                "company": slug,
                "title": title,
                "location": location,
                "url": job_url,
                "posted_date": job.get("published_on"),
                "ats": "Workable"
            })
        return results
    except Exception as e:
        log.debug(f"Workable {slug}: {e}")
        return []


ATS_FETCHERS = [fetch_ashby, fetch_greenhouse, fetch_lever, fetch_workable]

# --- Filter ---

def matches_title(title: str) -> bool:
    lower = title.lower()
    return any(kw in lower for kw in TITLE_KEYWORDS)

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
        os.system('git config user.name "Aline News Agent"')
        os.system("git add demand_sent_urls.json")
        os.system('git commit -m "chore: update demand_sent_urls"')
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

def format_finding(f: dict) -> str:
    return (
        f"\U0001f3af *{f['title']} \u2014 {f['company']} ({f['ats']})*\n"
        f"Location: {f['location'] or 'Not specified'}\n"
        f"<{f['url']}|View job>"
    )

def format_summary(companies: int, jobs: int) -> str:
    now = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M")
    return (
        f"\u2705 *Demand Agent \u2014 {now}*\n"
        f"Companies scanned: {companies} | Executive roles found: {jobs}"
    )

# --- Main ---

def main():
    sent_urls = load_sent_urls()
    jobs_found = 0
    companies_scanned = set()

    for slug in COMPANIES:
        log.info(f"Scanning {slug}")
        for fetcher in ATS_FETCHERS:
            jobs = fetcher(slug)
            if jobs:
                companies_scanned.add(slug)
                for job in jobs:
                    url = job.get("url", "")
                    if not url or url in sent_urls:
                        continue
                    send_slack(format_finding(job))
                    sent_urls.add(url)
                    jobs_found += 1
                    log.info(f"  Found: {job['title']} at {slug} ({job['ats']})")
        time.sleep(0.3)  # Polite rate limiting between companies

    send_slack(format_summary(len(COMPANIES), jobs_found))
    save_sent_urls(sent_urls)
    commit_sent_urls()
    log.info(f"Done. Companies with hits: {len(companies_scanned)} | Executive roles: {jobs_found}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"Fatal error: {e}")
        try:
            send_slack(f"\u26a0\ufe0f *Demand Agent Error*\n{e}")
        except Exception:
            pass
        raise
