from __future__ import annotations

import os
import json
import sqlite3
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ats_slugs.db")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Aline-Slug-Discovery/1.0"})

# --- Slug variations ---

def generate_slugs(company: str) -> list[str]:
    """Generate 5 smart slug variations — covers 95% of real matches."""
    c = company.strip().lower()
    # Remove common suffixes from company names
    for strip in [" gmbh", " ag", " se", " inc", " ltd", " holding"]:
        c = c.replace(strip, "")
    c = c.strip()

    base = c.replace(" ", "-")       # "alpine eagle" -> "alpine-eagle"
    nospace = c.replace(" ", "")      # "alpine eagle" -> "alpineeagle"
    nodash = base.replace("-", "")    # same as nospace for most

    slugs = []
    seen = set()
    for s in [base, nospace, nodash, base + "-gmbh", base + "-io"]:
        if s not in seen:
            slugs.append(s)
            seen.add(s)
    return slugs

# --- ATS probe functions (just check if slug exists, return job count) ---

def probe_ashby(slug: str) -> dict | None:
    try:
        resp = SESSION.get(
            f"https://api.ashbyhq.com/posting-public/job-board/{slug}",
            timeout=3
        )
        if resp.status_code in (401, 404, 400):
            return None
        resp.raise_for_status()
        data = resp.json()
        jobs = data.get("jobPostings", data.get("jobs", []))
        return {"platform": "ashby", "slug": slug, "job_count": len(jobs)}
    except Exception:
        return None


def probe_greenhouse(slug: str) -> dict | None:
    try:
        resp = SESSION.get(
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
            timeout=3
        )
        if resp.status_code in (404, 400):
            return None
        resp.raise_for_status()
        data = resp.json()
        jobs = data.get("jobs", [])
        return {"platform": "greenhouse", "slug": slug, "job_count": len(jobs)}
    except Exception:
        return None


def probe_lever(slug: str) -> dict | None:
    try:
        resp = SESSION.get(
            f"https://api.lever.co/v0/postings/{slug}?mode=json",
            timeout=3
        )
        if resp.status_code in (404, 400):
            return None
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            return None
        return {"platform": "lever", "slug": slug, "job_count": len(data)}
    except Exception:
        return None


def probe_workable(slug: str) -> dict | None:
    try:
        resp = SESSION.post(
            f"https://apply.workable.com/api/v3/accounts/{slug}/jobs",
            json={}, timeout=3
        )
        if resp.status_code in (404, 400):
            return None
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if len(results) == 0:
            return None  # Account exists but no jobs — not useful
        return {"platform": "workable", "slug": slug, "job_count": len(results)}
    except Exception:
        return None


def probe_bamboohr(slug: str) -> dict | None:
    try:
        resp = SESSION.get(
            f"https://{slug}.bamboohr.com/careers/list",
            timeout=3
        )
        if resp.status_code in (404, 400):
            return None
        resp.raise_for_status()
        # BambooHR returns HTML, check if there are job listings
        text = resp.text
        # Look for job data in the page — BambooHR embeds JSON or has job cards
        if '"jobOpenings"' in text or 'class="BambooHR-ATS-board' in text or 'ResumatorJob' in text:
            # Count rough number of job entries
            count = text.count('"id"')
            if count == 0:
                count = text.count('class="BambooHR-ATS-Department-Job"')
            if count > 0:
                return {"platform": "bamboohr", "slug": slug, "job_count": count}
        return None
    except Exception:
        return None


PROBES = [
    ("ashby", probe_ashby),
    ("greenhouse", probe_greenhouse),
    ("lever", probe_lever),
    ("workable", probe_workable),
    ("bamboohr", probe_bamboohr),
]

# --- Database ---

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS discovered_slugs (
            company TEXT NOT NULL,
            platform TEXT NOT NULL,
            slug TEXT NOT NULL,
            job_count INTEGER DEFAULT 0,
            discovered_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_checked TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (company, platform)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS failed_probes (
            company TEXT NOT NULL,
            platform TEXT NOT NULL,
            slug_tried TEXT NOT NULL,
            checked_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_discovered_platform
        ON discovered_slugs(platform)
    """)
    conn.commit()
    return conn


def already_discovered(conn, company: str, platform: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM discovered_slugs WHERE company = ? AND platform = ?",
        (company, platform)
    ).fetchone()
    return row is not None


def save_discovery(conn, company: str, platform: str, slug: str, job_count: int):
    conn.execute("""
        INSERT OR REPLACE INTO discovered_slugs (company, platform, slug, job_count, last_checked)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, (company, platform, slug, job_count))
    conn.commit()


def get_all_discovered(conn) -> list[dict]:
    rows = conn.execute(
        "SELECT company, platform, slug, job_count FROM discovered_slugs ORDER BY company"
    ).fetchall()
    return [{"company": r[0], "platform": r[1], "slug": r[2], "job_count": r[3]} for r in rows]


# --- Discovery logic ---

def discover_company(conn, company: str) -> list[dict]:
    """Try all slug variations across all platforms for a company."""
    slugs = generate_slugs(company)
    found = []

    for platform_name, probe_fn in PROBES:
        if already_discovered(conn, company, platform_name):
            log.debug(f"  {company}/{platform_name}: already discovered, skipping")
            continue

        hit = None
        for slug in slugs:
            result = probe_fn(slug)
            if result and result["job_count"] > 0:
                hit = result
                break
            time.sleep(0.05)  # Rate limiting between probes

        if hit:
            save_discovery(conn, company, hit["platform"], hit["slug"], hit["job_count"])
            found.append(hit)
            log.info(f"  FOUND: {company} -> {hit['platform']}/{hit['slug']} ({hit['job_count']} jobs)")
        else:
            log.debug(f"  {company}/{platform_name}: no match")

    return found


def load_companies(path: str) -> list[str]:
    """Load company list from a text file (one per line) or JSON array."""
    with open(path) as f:
        content = f.read().strip()
    if content.startswith("["):
        return json.loads(content)
    return [line.strip() for line in content.splitlines() if line.strip()]


# --- Main ---

def main():
    import sys

    if len(sys.argv) < 2:
        print("Usage: python slug_discovery.py <companies_file>")
        print("  File can be .txt (one company per line) or .json (array)")
        print("")
        print("Or: python slug_discovery.py --report")
        sys.exit(1)

    conn = init_db()

    if sys.argv[1] == "--report":
        results = get_all_discovered(conn)
        if not results:
            print("No slugs discovered yet.")
            return
        print(f"\n{'Company':<30} {'Platform':<12} {'Slug':<35} {'Jobs':>5}")
        print("-" * 85)
        for r in results:
            print(f"{r['company']:<30} {r['platform']:<12} {r['slug']:<35} {r['job_count']:>5}")
        print(f"\nTotal: {len(results)} discovered slugs")
        conn.close()
        return

    companies_file = sys.argv[1]
    companies = load_companies(companies_file)
    log.info(f"Loaded {len(companies)} companies from {companies_file}")

    total_found = 0
    for i, company in enumerate(companies):
        log.info(f"[{i+1}/{len(companies)}] Discovering {company}")
        found = discover_company(conn, company)
        total_found += len(found)
        time.sleep(0.1)  # Between companies

    log.info(f"Discovery complete. New slugs found: {total_found}")

    # Print summary
    all_discovered = get_all_discovered(conn)
    log.info(f"Total discovered slugs in database: {len(all_discovered)}")
    conn.close()


if __name__ == "__main__":
    main()
