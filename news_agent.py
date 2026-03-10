import os
import json
import time
import logging
from datetime import datetime, timezone

import anthropic
import requests
from tavily import TavilyClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- Config ---
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
TAVILY_API_KEY = os.environ["TAVILY_API_KEY"]
SLACK_WEBHOOK = os.environ["SLACK_WEBHOOK_NEWS"]
SENT_URLS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sent_urls.json")
MAX_ITERATIONS = 16  # message pairs cap (~6 tool calls)

# --- Clients ---
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
tavily = TavilyClient(api_key=TAVILY_API_KEY)

# --- System prompt ---
def load_system_prompt():
    base = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(base, "soul.md")) as f:
        soul = f.read()
    with open(os.path.join(base, "skill.md")) as f:
        skill = f.read()
    return soul + "\n\n---\n\n" + skill

# --- Tools ---
TOOLS = [
    {
        "name": "tavily_search",
        "description": "Search the web for recent news and information.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "tavily_search_news",
        "description": "Search the news index for recent articles.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"}
            },
            "required": ["query"]
        }
    }
]

def tavily_search(query: str) -> list[dict]:
    try:
        resp = tavily.search(query=query, max_results=5)
        return [
            {
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "snippet": r.get("content", ""),
                "published_date": r.get("published_date")
            }
            for r in resp.get("results", [])
        ]
    except Exception as e:
        log.error(f"Tavily search error: {e}")
        return []

def tavily_search_news(query: str) -> list[dict]:
    try:
        resp = tavily.search(query=query, max_results=5, topic="news")
        return [
            {
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "snippet": r.get("content", ""),
                "published_date": r.get("published_date")
            }
            for r in resp.get("results", [])
        ]
    except Exception as e:
        log.error(f"Tavily news search error: {e}")
        return []

TOOL_MAP = {
    "tavily_search": tavily_search,
    "tavily_search_news": tavily_search_news,
}

def execute_tools(content_blocks) -> list[dict]:
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
                "content": json.dumps(output)
            })
    return results

# --- ReAct prompt ---
REACT_PROMPT = """You are Aline's News Agent. Your job is to scan the DACH market right now and find executive signals that are relevant for Aline.

You have two tools:
- tavily_search(query) — general web search
- tavily_search_news(query) — news-specific search

Use the ReAct pattern:
- THOUGHT: reason about what to search and why
- ACTION: call one tool with a specific query
- OBSERVATION: read the results
- Repeat until you have covered all five signal types

Signal types to cover:
1. CEO / C-Level Departures in DACH
2. PE Deals & Acquisitions in DACH
3. Funding Rounds Series A+ in DACH
4. Restructuring & Insolvencies in DACH
5. International Expansion into or from DACH

Rules:
- Generate your own queries. Do not use generic terms. Be specific.
- If a result is promising, search deeper. Follow the thread.
- Avoid duplicates. If you already found a story, do not search for it again.
- Maximum 6 tool calls per run. Use them wisely. Combine signal types into broader queries.
- When done, output a JSON array of findings. Nothing else.

Output format (JSON array):
[
  {
    "signal_type": "CEO Departure | PE Deal | Funding Round | Restructuring | International Expansion",
    "priority": "hot | watch",
    "company": "Company name",
    "summary": "One sentence: what happened.",
    "why_relevant": "One sentence: why this matters for Aline.",
    "url": "https://...",
    "published_date": "YYYY-MM-DD or null"
  }
]

Only include findings with priority "hot" or "watch". Ignore everything else.
Output ONLY the JSON array. No text before or after."""

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
        os.system("git add sent_urls.json")
        os.system('git commit -m "chore: update sent_urls"')
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

def format_hot(finding: dict) -> str:
    return (
        f"\U0001f525 *{finding['signal_type']} \u2014 {finding['company']}*\n"
        f"{finding['summary']}\n"
        f"{finding['why_relevant']}\n"
        f"<{finding['url']}|Read article>"
    )

def format_watch(finding: dict) -> str:
    return (
        f"\U0001f4cc *{finding['signal_type']} \u2014 {finding['company']}*\n"
        f"{finding['summary']}\n"
        f"{finding['why_relevant']}\n"
        f"<{finding['url']}|Read article>"
    )

def format_summary(queries: int, articles: int, hot: int, watch: int) -> str:
    now = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M")
    return (
        f"\u2705 *News Agent \u2014 {now}*\n"
        f"Queries run: {queries} | Articles reviewed: {articles} | Hot: {hot} | Watch: {watch}"
    )

# --- Claude API call with retry ---
def call_claude(system_prompt, messages):
    for attempt in range(3):
        try:
            response = claude.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=4096,
                system=system_prompt,
                tools=TOOLS,
                messages=messages
            )
            return response
        except Exception as e:
            if attempt == 2:
                raise
            wait = 15 * (attempt + 1)  # 15s, 30s — respect 30k tokens/min limit
            log.warning(f"Claude API error (attempt {attempt+1}): {e}. Retrying in {wait}s...")
            time.sleep(wait)

# --- Extract JSON from response ---
def extract_findings(response) -> list[dict]:
    for block in response.content:
        if hasattr(block, "text"):
            text = block.text.strip()
            # Find JSON array in the text
            start = text.find("[")
            end = text.rfind("]")
            if start != -1 and end != -1:
                try:
                    return json.loads(text[start:end+1])
                except json.JSONDecodeError:
                    pass
    return []

# --- Main ---
def main():
    system_prompt = load_system_prompt()
    sent_urls = load_sent_urls()
    messages = [{"role": "user", "content": REACT_PROMPT}]

    query_count = 0
    article_count = 0

    log.info("Starting ReAct loop")

    while len(messages) < MAX_ITERATIONS:
        if len(messages) > 1:
            time.sleep(5)  # Rate limit: 30k tokens/min
        response = call_claude(system_prompt, messages)

        if response.stop_reason == "end_turn":
            log.info("Agent finished (end_turn)")
            break

        if response.stop_reason == "tool_use":
            # Count queries and articles
            for block in response.content:
                if block.type == "tool_use":
                    query_count += 1

            tool_results = execute_tools(response.content)

            # Count articles from results
            for tr in tool_results:
                try:
                    results = json.loads(tr["content"])
                    if isinstance(results, list):
                        article_count += len(results)
                except (json.JSONDecodeError, TypeError):
                    pass

            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})
            continue

        # Unexpected stop reason
        log.warning(f"Unexpected stop_reason: {response.stop_reason}")
        break

    # Parse findings from last response
    findings = extract_findings(response)
    log.info(f"Findings: {len(findings)}")

    hot_count = 0
    watch_count = 0

    for f in findings:
        url = f.get("url", "")
        if url in sent_urls:
            log.info(f"Skipping duplicate: {url}")
            continue

        priority = f.get("priority", "").lower()
        if priority == "hot":
            send_slack(format_hot(f))
            hot_count += 1
            sent_urls.add(url)
        elif priority == "watch":
            send_slack(format_watch(f))
            watch_count += 1
            sent_urls.add(url)

    # Always send run summary
    send_slack(format_summary(query_count, article_count, hot_count, watch_count))

    # Persist sent URLs
    save_sent_urls(sent_urls)
    commit_sent_urls()

    log.info("Done")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"Fatal error: {e}")
        try:
            send_slack(f"\u26a0\ufe0f *News Agent Error*\n{e}")
        except Exception:
            pass
        raise
