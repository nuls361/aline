"""
Aline Slack Bot — paste a JD URL, get the outreach email back in-thread.

Uses Slack Bolt with Socket Mode (no public URL needed).

Required env vars:
  SLACK_BOT_TOKEN    — xoxb-... (Bot User OAuth Token)
  SLACK_APP_TOKEN    — xapp-... (App-Level Token with connections:write)
  ANTHROPIC_API_KEY
  TAVILY_API_KEY
  APOLLO_API_KEY     — optional
"""

import os
import re
import logging
import anthropic
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# Reuse pipeline functions from dry_run
import dry_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- Config ---
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_APP_TOKEN = os.environ.get("SLACK_APP_TOKEN", "")

# Initialize dry_run's claude client so pipeline functions work
dry_run.claude = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

# Job board URL pattern — Ashby, Greenhouse, Lever, LinkedIn, etc.
JD_URL_PATTERN = re.compile(
    r"https?://(?:"
    r"jobs\.ashbyhq\.com|"
    r"(?:job-)?boards\.greenhouse\.io|"
    r"jobs\.lever\.co|"
    r"[\w.-]+\.recruitee\.com|"
    r"[\w.-]+\.workable\.com|"
    r"www\.linkedin\.com/jobs/view|"
    r"join\.com/companies"
    r")/\S+"
)

app = App(token=SLACK_BOT_TOKEN)

# Track processed message timestamps to avoid duplicates
_processed_ts = set()


def run_pipeline(url: str) -> dict:
    """Run the full Aline pipeline for a JD URL. Returns result dict."""
    result = {"ok": False}

    # Step 1: Fetch JD
    jd = dry_run.fetch_jd(url)
    if not jd.get("title") and not jd.get("description"):
        result["error"] = "Could not extract JD content from this URL."
        return result

    # Step 2: Enrich company
    company_info = {}
    if jd.get("company"):
        company_info = dry_run.enrich_company(jd["company"], jd.get("company_url", ""))

    # Step 3: Classify role
    role_info = dry_run.classify_role(jd)

    # Step 4: Find decision maker
    domain = company_info.get("domain", "")
    dm = dry_run.find_decision_maker(
        company_name=jd.get("company", ""),
        domain=domain,
        role_info=role_info,
        skip_apollo=False,
    )

    # Step 5: Generate email
    email_data = dry_run.generate_email(jd, company_info, role_info, dm)

    if not email_data:
        result["error"] = "Email generation failed."
        return result

    result["ok"] = True
    result["jd"] = jd
    result["company_info"] = company_info
    result["role_info"] = role_info
    result["dm"] = dm
    result["email"] = email_data
    return result


def format_slack_reply(result: dict) -> str:
    """Format pipeline result as a Slack message."""
    jd = result["jd"]
    company_info = result["company_info"]
    role_info = result["role_info"]
    dm = result["dm"]
    email = result["email"]

    lines = []
    lines.append(f"*{jd.get('title', '?')}* at *{jd.get('company', '?')}*")
    lines.append(f"_{company_info.get('one_liner', '')}_ · {company_info.get('funding_stage', '?')} · ~{company_info.get('headcount', '?')} people")
    lines.append("")
    lines.append(f"📍 {jd.get('location', '?')} · 🏷️ {role_info.get('signal_type', '?')} · {role_info.get('engagement_type', '?')}")
    lines.append(f"👤 *{dm.get('name', '?')}* — {dm.get('title', '?')}")
    if dm.get("email"):
        lines.append(f"✉️ {dm['email']}")
    if dm.get("linkedin"):
        lines.append(f"🔗 {dm['linkedin']}")
    lines.append("")
    lines.append("─" * 40)
    lines.append(f"*Subject:* {email.get('subject', '?')}")
    lines.append("")
    lines.append(email.get("body", ""))
    lines.append("─" * 40)
    lines.append(f"_💡 {email.get('reasoning', '')}_")

    return "\n".join(lines)


@app.event("message")
def handle_message(event, say):
    """Listen for messages containing JD URLs."""
    subtype = event.get("subtype")

    # For message_changed (Slack URL unfurling), extract from the edited message
    if subtype == "message_changed":
        message = event.get("message", {})
        # Ignore if the edit is from a bot
        if message.get("bot_id"):
            return
        text = message.get("text", "")
        ts = message.get("ts", event.get("ts", ""))
    elif subtype:
        # Ignore other subtypes (bot messages, deletions, etc.)
        return
    else:
        text = event.get("text", "")
        ts = event.get("ts", "")

    channel = event.get("channel", "")
    log.info(f"Message received: subtype={subtype} text={text[:200]}")

    # Dedup: skip if we already processed this message
    if ts in _processed_ts:
        return
    _processed_ts.add(ts)
    # Keep set from growing forever
    if len(_processed_ts) > 1000:
        _processed_ts.clear()

    # Slack wraps URLs like <https://...|label> or <https://...>
    # Extract raw URLs from Slack's formatting first
    slack_urls = re.findall(r"<(https?://[^|>]+)(?:\|[^>]*)?>", text)
    raw_text = text  # also check the raw text in case
    all_urls = slack_urls + JD_URL_PATTERN.findall(raw_text)

    # Filter to JD URLs only
    urls = []
    for u in all_urls:
        if JD_URL_PATTERN.match(u):
            urls.append(u)

    log.info(f"URLs found: {urls}")

    if not urls:
        return

    # Deduplicate
    urls = list(dict.fromkeys(urls))

    for url in urls:
        log.info(f"Processing JD URL: {url}")

        # Post a "working on it" message in-thread
        say(text="⏳ Running pipeline...", thread_ts=ts)

        try:
            result = run_pipeline(url)
            if result["ok"]:
                reply = format_slack_reply(result)
                say(text=reply, thread_ts=ts)
            else:
                say(text=f"❌ {result.get('error', 'Unknown error')}", thread_ts=ts)
        except Exception as e:
            log.error(f"Pipeline failed: {e}", exc_info=True)
            say(text=f"❌ Pipeline error: {e}", thread_ts=ts)


if __name__ == "__main__":
    if not SLACK_BOT_TOKEN:
        print("❌ SLACK_BOT_TOKEN not set")
        exit(1)
    if not SLACK_APP_TOKEN:
        print("❌ SLACK_APP_TOKEN not set")
        exit(1)

    print("🤖 Aline Slack Bot starting...")
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    handler.start()
