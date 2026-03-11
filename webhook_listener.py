"""
Aline Webhook Listener — FastAPI server for Instantly engagement events.

Receives email_opened, link_clicked, replied, bounced events from Instantly.
Uses Claude to decide intelligent follow-ups. Updates Attio CRM and alerts Slack.

Run: uvicorn webhook_listener:app --host 0.0.0.0 --port 8080
"""

import os
import json
import sqlite3
import logging
import time
from datetime import datetime, timezone, timedelta

import anthropic
import requests
from fastapi import FastAPI, Request

from attio_client import attio

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- Config ---
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
INSTANTLY_API_KEY = os.environ["INSTANTLY_API_KEY"]
SLACK_WEBHOOK = os.environ["SLACK_WEBHOOK_HOT_LEADS"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LEADS_DB_PATH = os.path.join(BASE_DIR, "leads.db")

# --- Clients ---
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
SESSION = requests.Session()

app = FastAPI(title="Aline Webhook Listener")

# --- SQLite local state ---

def init_db():
    conn = sqlite3.connect(LEADS_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS engagement_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_email TEXT NOT NULL,
            company_name TEXT DEFAULT '',
            event_type TEXT NOT NULL,
            event_at TEXT DEFAULT CURRENT_TIMESTAMP,
            open_count INTEGER DEFAULT 0,
            clicked INTEGER DEFAULT 0,
            replied INTEGER DEFAULT 0,
            last_followup_at TEXT,
            original_subject TEXT DEFAULT '',
            first_send_at TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_lead_email
        ON engagement_events(lead_email)
    """)
    conn.commit()
    return conn


def get_lead_state(conn, email: str) -> dict:
    """Get aggregated state for a lead."""
    row = conn.execute("""
        SELECT
            lead_email,
            company_name,
            MAX(CASE WHEN event_type = 'email_opened' THEN 1 ELSE 0 END) as has_opened,
            SUM(CASE WHEN event_type = 'email_opened' THEN 1 ELSE 0 END) as open_count,
            MAX(CASE WHEN event_type = 'link_clicked' THEN 1 ELSE 0 END) as clicked,
            MAX(CASE WHEN event_type = 'replied' THEN 1 ELSE 0 END) as replied,
            MAX(last_followup_at) as last_followup_at,
            MAX(original_subject) as original_subject,
            MIN(first_send_at) as first_send_at
        FROM engagement_events
        WHERE lead_email = ?
        GROUP BY lead_email
    """, (email,)).fetchone()

    if not row:
        return {
            "email": email,
            "open_count": 0,
            "clicked": False,
            "replied": False,
            "last_followup_at": None,
            "original_subject": "",
            "first_send_at": None,
        }

    return {
        "email": row["lead_email"],
        "company_name": row["company_name"] or "",
        "open_count": row["open_count"] or 0,
        "clicked": bool(row["clicked"]),
        "replied": bool(row["replied"]),
        "last_followup_at": row["last_followup_at"],
        "original_subject": row["original_subject"] or "",
        "first_send_at": row["first_send_at"],
    }


def log_event(conn, email: str, event_type: str, company_name: str = "",
              original_subject: str = "", first_send_at: str = ""):
    """Log an engagement event."""
    conn.execute("""
        INSERT INTO engagement_events (lead_email, company_name, event_type,
            original_subject, first_send_at)
        VALUES (?, ?, ?, ?, ?)
    """, (email, company_name, event_type, original_subject, first_send_at))
    conn.commit()


def update_followup_time(conn, email: str):
    """Mark that a follow-up was sent."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        UPDATE engagement_events SET last_followup_at = ?
        WHERE lead_email = ? AND last_followup_at IS NULL
    """, (now, email))
    conn.commit()


# --- Claude follow-up reasoning ---

FOLLOWUP_PROMPT = """Lead: {name}, {title} at {company}
Engagement event: {event_type}
Event details: {event_details}
Original email subject: {original_subject}
Days since first email: {days_since_send}
Open count: {open_count}

You are Aline's outreach agent. Decide what follow-up to send, if any.
Be concise. Reference the engagement signal naturally but not creepily.
Max 3 sentences. One clear CTA.

If no follow-up is warranted, return: {{"send": false, "reason": "..."}}
If follow-up warranted, return:
{{
  "send": true,
  "subject": "Re: ...",
  "body": "...",
  "reasoning": "..."
}}"""


def decide_followup(lead_state: dict, event_type: str, event_details: str = "") -> dict | None:
    """Ask Claude whether to send a follow-up."""
    days_since = 0
    if lead_state.get("first_send_at"):
        try:
            first = datetime.fromisoformat(lead_state["first_send_at"].replace("Z", "+00:00"))
            days_since = (datetime.now(timezone.utc) - first).days
        except (ValueError, TypeError):
            pass

    prompt = FOLLOWUP_PROMPT.format(
        name=lead_state.get("name", lead_state["email"]),
        title=lead_state.get("title", ""),
        company=lead_state.get("company_name", ""),
        event_type=event_type,
        event_details=event_details,
        original_subject=lead_state.get("original_subject", ""),
        days_since_send=days_since,
        open_count=lead_state.get("open_count", 0),
    )

    try:
        response = claude.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=512,
            system="You are Aline's outreach agent. Return only valid JSON.",
            messages=[{"role": "user", "content": prompt}],
        )
        for block in response.content:
            if hasattr(block, "text"):
                text = block.text.strip()
                start = text.find("{")
                end = text.rfind("}")
                if start != -1 and end != -1:
                    return json.loads(text[start:end+1])
    except Exception as e:
        log.error(f"Claude followup error: {e}")

    return None


# --- Instantly send follow-up ---

def send_followup_instantly(email: str, subject: str, body: str) -> bool:
    """Send a follow-up via Instantly API."""
    try:
        resp = SESSION.post(
            "https://api.instantly.ai/api/v1/lead/add",
            json={
                "api_key": INSTANTLY_API_KEY,
                "email": email,
                "custom_variables": {
                    "subject": subject,
                    "body": body,
                },
            },
            timeout=15,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error(f"Instantly follow-up error for {email}: {e}")
        return False


# --- Slack ---

def send_slack(text: str):
    try:
        resp = requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"Slack error: {e}")


# --- Attio helpers ---

def update_role_stage(email: str, stage: str):
    """Find role linked to this email and update sales_stage."""
    people = attio.search_records("people", query=email, limit=1)
    if not people:
        return
    # For now, log the stage update — exact role linkage depends on Attio schema
    log.info(f"Would update role stage to '{stage}' for {email}")


# --- Webhook endpoint ---

@app.post("/webhook/instantly")
async def handle_instantly_webhook(request: Request):
    """Handle Instantly engagement events."""
    try:
        payload = await request.json()
    except Exception:
        return {"status": "error", "message": "invalid JSON"}

    event_type = payload.get("event_type", payload.get("event", ""))
    email = payload.get("lead_email", payload.get("email", ""))
    company = payload.get("company_name", "")
    name = payload.get("lead_name", payload.get("name", ""))
    title = payload.get("lead_title", "")
    original_subject = payload.get("subject", "")
    event_details = payload.get("details", "")

    if not email or not event_type:
        return {"status": "error", "message": "missing email or event_type"}

    log.info(f"Webhook: {event_type} from {email} ({company})")

    conn = init_db()
    log_event(conn, email, event_type, company_name=company, original_subject=original_subject)
    lead_state = get_lead_state(conn, email)
    lead_state["name"] = name
    lead_state["title"] = title

    # --- Event handling ---

    if event_type == "email_opened":
        # Check if opened >= 2 and no reply within 48h
        if lead_state["open_count"] >= 2 and not lead_state["replied"]:
            # Check if no recent follow-up
            if not lead_state["last_followup_at"]:
                decision = decide_followup(lead_state, event_type, event_details)
                if decision and decision.get("send"):
                    sent = send_followup_instantly(email, decision["subject"], decision["body"])
                    if sent:
                        update_followup_time(conn, email)
                        log.info(f"Follow-up sent to {email}: {decision['subject']}")

    elif event_type == "link_clicked":
        # High intent — immediate action
        send_slack(
            f"\U0001f525 *{name or email} at {company} clicked your link*\n"
            f"Subject: {original_subject}"
        )
        update_role_stage(email, "engaged")

        decision = decide_followup(lead_state, event_type, event_details)
        if decision and decision.get("send"):
            sent = send_followup_instantly(email, decision["subject"], decision["body"])
            if sent:
                update_followup_time(conn, email)

    elif event_type == "replied":
        # Human takes over — no automated follow-up
        reply_preview = (event_details or "")[:200]
        send_slack(
            f"\U0001f4ac *Reply from {name or email} at {company}*\n"
            f"Subject: {original_subject}\n"
            f"Preview: {reply_preview}"
        )
        update_role_stage(email, "replied")

    elif event_type == "bounced":
        # Mark email as invalid
        log.warning(f"Bounced: {email}")
        attio_resp = attio.search_records("people", query=email, limit=1)
        if attio_resp:
            person_id = attio.extract_record_id(attio_resp[0])
            if person_id:
                attio.create_note(
                    parent_object="people",
                    parent_record_id=person_id,
                    title="Email bounced",
                    content=f"Email {email} bounced. Marked as invalid.",
                )

    elif event_type == "unsubscribed":
        log.info(f"Unsubscribed: {email}")

    conn.close()
    return {"status": "ok", "event": event_type}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "aline-webhook-listener"}
