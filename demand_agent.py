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
    "personio", "celonis", "forto", "billie", "taxfix", "gorillas", "sennder",
    "moonfare", "penta", "sumup", "tier", "comtravo", "planity", "staffbase",
    "contentful", "mambu", "relayr", "adjustcom", "homeday", "simplesystem"
]

TITLE_KEYWORDS = [
    "head of", "vp", "vice president", "chief", "director", "interim",
    "fractional", "managing director", "general manager", "c-level"
]

ASHBY_URL = "https://api.ashbyhq.com/posting-public/job-board/{slug}"

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

# --- Ashby scraping ---
def fetch_jobs(slug: str) -> list[dict]:
    try:
        resp = requests.get(ASHBY_URL.format(slug=slug), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        jobs = data.get("jobs", [])
        results = []
        for job in jobs:
            title = job.get("title", "")
            if matches_title(title):
                results.append({
                    "company": slug,
                    "title": title,
                    "location": job.get("location", ""),
                    "url": job.get("jobUrl", ""),
                    "posted_date": job.get("publishedAt", None)
                })
        return results
    except Exception as e:
        log.error(f"Error fetching {slug}: {e}")
        return []

def matches_title(title: str) -> bool:
    lower = title.lower()
    return any(kw in lower for kw in TITLE_KEYWORDS)

# --- Slack ---
def send_slack(text: str):
    try:
        resp = requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"Slack error: {e}")

def format_finding(finding: dict) -> str:
    return (
        f"\U0001f3af *{finding['title']} \u2014 {finding['company']}*\n"
        f"Location: {finding['location']}\n"
        f"<{finding['url']}|View job>"
    )

def format_summary(companies: int, jobs: int) -> str:
    now = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M")
    return (
        f"\u2705 *Demand Agent \u2014 {now}*\n"
        f"Companies scanned: {companies} | Jobs found: {jobs}"
    )

# --- Main ---
def main():
    sent_urls = load_sent_urls()
    jobs_found = 0

    for slug in COMPANIES:
        log.info(f"Scanning {slug}")
        jobs = fetch_jobs(slug)
        for job in jobs:
            url = job.get("url", "")
            if url in sent_urls:
                log.info(f"Skipping duplicate: {url}")
                continue
            send_slack(format_finding(job))
            sent_urls.add(url)
            jobs_found += 1
        time.sleep(0.5)  # Be polite to Ashby API

    send_slack(format_summary(len(COMPANIES), jobs_found))
    save_sent_urls(sent_urls)
    commit_sent_urls()
    log.info(f"Done. Jobs found: {jobs_found}")

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
