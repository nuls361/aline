#!/usr/bin/env python3
"""
Attio API Client — Drop-in replacement for Supabase REST calls.

Usage across all modules:
    from attio_client import attio

    # Upsert a company
    company = attio.upsert_company(domain="acme.com", values={
        "name": [{"value": "Acme Corp"}],
        "company_type": attio.format_select("prospect"),
    })

    # Query with filters
    hot_roles = attio.query_records("roles", filter={
        "sales_stage": {"$eq": "new"},
    })

    # Create a note on a company
    attio.create_note("companies", record_id, title="Enrichment", content="...")

API Reference:
    Base URL: https://api.attio.com/v2
    Auth: Bearer token
    Rate limits: 100 reads/s, 25 writes/s
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

import requests

logger = logging.getLogger("attio_client")

ATTIO_API_KEY = os.getenv("ATTIO_API_KEY", "")
ATTIO_BASE_URL = "https://api.attio.com/v2"


class AttioClient:
    """Thin wrapper around Attio REST API v2."""

    def __init__(self, api_key: str = "", base_url: str = ATTIO_BASE_URL):
        self.api_key = api_key or ATTIO_API_KEY
        self.base_url = base_url
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        })

    # ═══════════════════════════════════════════════════════════
    # LOW-LEVEL REQUEST
    # ═══════════════════════════════════════════════════════════

    def _request(
        self,
        method: str,
        path: str,
        json_data: Optional[dict] = None,
        params: Optional[dict] = None,
        timeout: int = 15,
        retries: int = 3,
    ) -> Optional[dict]:
        """
        Make an authenticated request to Attio API with retry on 429.
        Returns parsed JSON response or None on failure.
        """
        url = f"{self.base_url}{path}"

        for attempt in range(retries):
            try:
                resp = self._session.request(
                    method, url, json=json_data, params=params, timeout=timeout,
                )

                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("Retry-After", 2))
                    logger.warning(f"Rate limited. Retry after {retry_after}s (attempt {attempt + 1}/{retries})")
                    time.sleep(retry_after)
                    continue

                if resp.status_code in (200, 201):
                    return resp.json()

                if resp.status_code == 204:
                    return {"ok": True}

                logger.error(f"Attio {method} {path}: {resp.status_code} — {resp.text[:300]}")
                return None

            except requests.exceptions.Timeout:
                logger.warning(f"Attio timeout on {method} {path} (attempt {attempt + 1}/{retries})")
                if attempt < retries - 1:
                    time.sleep(1)
                continue
            except Exception as e:
                logger.error(f"Attio request error: {e}")
                return None

        logger.error(f"Attio {method} {path}: exhausted {retries} retries")
        return None

    # ═══════════════════════════════════════════════════════════
    # OBJECTS & ATTRIBUTES (setup)
    # ═══════════════════════════════════════════════════════════

    def list_objects(self) -> Optional[list]:
        """List all objects in the workspace."""
        resp = self._request("GET", "/objects")
        return resp.get("data") if resp else None

    def get_object(self, object_slug: str) -> Optional[dict]:
        """Get a single object by slug."""
        return self._request("GET", f"/objects/{object_slug}")

    def create_object(self, api_slug: str, singular_noun: str, plural_noun: str) -> Optional[dict]:
        """Create a custom object."""
        return self._request("POST", "/objects", json_data={
            "data": {
                "api_slug": api_slug,
                "singular_noun": singular_noun,
                "plural_noun": plural_noun,
            }
        })

    def list_attributes(self, target: str, identifier: str) -> Optional[list]:
        """
        List attributes on an object or list.
        target: 'objects' or 'lists'
        identifier: object slug or list slug
        """
        resp = self._request("GET", f"/{target}/{identifier}/attributes")
        return resp.get("data") if resp else None

    def create_attribute(
        self,
        target: str,
        identifier: str,
        title: str,
        api_slug: str,
        attr_type: str,
        description: str = "",
        config: Optional[dict] = None,
        is_required: bool = False,
        is_unique: bool = False,
        is_multiselect: bool = False,
        relationship: Optional[dict] = None,
    ) -> Optional[dict]:
        """
        Create an attribute on an object or list.

        target: 'objects' or 'lists'
        identifier: object/list slug
        attr_type: 'text', 'number', 'select', 'status', 'checkbox',
                   'date', 'currency', 'domain', 'email-address',
                   'phone-number', 'record-reference', 'rating',
                   'timestamp', 'location', 'personal-name'
        description: Required by Attio API. Defaults to title if empty.
        config: type-specific config (e.g. select options, status stages)
        relationship: for record-reference, e.g. {"target_object": "companies"}
        """
        payload = {
            "data": {
                "title": title,
                "description": description or title,
                "api_slug": api_slug,
                "type": attr_type,
                "is_required": is_required,
                "is_unique": is_unique,
                "is_multiselect": is_multiselect,
                "config": config if config else {},
            }
        }
        if relationship:
            payload["data"]["relationship"] = relationship

        return self._request("POST", f"/{target}/{identifier}/attributes", json_data=payload)

    def create_select_attribute(
        self,
        target: str,
        identifier: str,
        title: str,
        api_slug: str,
        options: list[str],
        description: str = "",
        is_multiselect: bool = False,
    ) -> Optional[dict]:
        """Convenience: create a select attribute with predefined options."""
        return self.create_attribute(
            target=target,
            identifier=identifier,
            title=title,
            api_slug=api_slug,
            description=description or title,
            attr_type="select",
            is_multiselect=is_multiselect,
            config={"options": [{"title": opt} for opt in options]},
        )

    # ═══════════════════════════════════════════════════════════
    # RECORDS — CRUD
    # ═══════════════════════════════════════════════════════════

    def create_record(self, object_slug: str, values: dict) -> Optional[dict]:
        """Create a new record. Throws on unique conflicts — use upsert_record instead."""
        return self._request("POST", f"/objects/{object_slug}/records", json_data={
            "data": {"values": values}
        })

    def upsert_record(self, object_slug: str, matching_attribute: str, values: dict) -> Optional[dict]:
        """
        Assert (upsert) a record. If a record with the same matching_attribute exists,
        it gets updated. Otherwise a new record is created.
        """
        return self._request(
            "PUT",
            f"/objects/{object_slug}/records",
            params={"matching_attribute": matching_attribute},
            json_data={"data": {"values": values}},
        )

    def get_record(self, object_slug: str, record_id: str) -> Optional[dict]:
        """Get a single record by ID."""
        return self._request("GET", f"/objects/{object_slug}/records/{record_id}")

    def update_record(self, object_slug: str, record_id: str, values: dict) -> Optional[dict]:
        """Update specific attributes on a record."""
        return self._request(
            "PATCH",
            f"/objects/{object_slug}/records/{record_id}",
            json_data={"data": {"values": values}},
        )

    def delete_record(self, object_slug: str, record_id: str) -> Optional[dict]:
        """Delete a record."""
        return self._request("DELETE", f"/objects/{object_slug}/records/{record_id}")

    # ═══════════════════════════════════════════════════════════
    # RECORDS — QUERY
    # ═══════════════════════════════════════════════════════════

    def query_records(
        self,
        object_slug: str,
        filter: Optional[dict] = None,
        sorts: Optional[list] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Optional[list]:
        """
        Query records with optional filters and sorts.
        POST /v2/objects/{object}/records/query
        """
        payload = {"limit": limit, "offset": offset}
        if filter:
            payload["filter"] = filter
        if sorts:
            payload["sorts"] = sorts

        resp = self._request("POST", f"/objects/{object_slug}/records/query", json_data=payload)
        return resp.get("data") if resp else None

    def search_records(self, object_slug: str, query: str, limit: int = 20) -> Optional[list]:
        """Full-text search across record attributes."""
        resp = self._request("POST", f"/objects/{object_slug}/records/search", json_data={
            "query": query, "limit": limit,
        })
        return resp.get("data") if resp else None

    def query_all(
        self,
        object_slug: str,
        filter: Optional[dict] = None,
        sorts: Optional[list] = None,
        batch_size: int = 50,
        max_records: int = 500,
    ) -> list:
        """Paginate through all matching records. Returns a flat list."""
        all_records = []
        offset = 0
        while offset < max_records:
            batch = self.query_records(object_slug, filter=filter, sorts=sorts, limit=batch_size, offset=offset)
            if not batch:
                break
            all_records.extend(batch)
            if len(batch) < batch_size:
                break
            offset += batch_size
        return all_records

    # ═══════════════════════════════════════════════════════════
    # CONVENIENCE — COMPANIES
    # ═══════════════════════════════════════════════════════════

    def upsert_company(self, domain: str, values: Optional[dict] = None) -> Optional[dict]:
        """Upsert a company by domain. Merges provided values."""
        payload = {"domains": [{"domain": domain}]}
        if values:
            payload.update(values)
        return self.upsert_record("companies", "domains", payload)

    def get_company_by_domain(self, domain: str) -> Optional[dict]:
        """Find a company by domain."""
        results = self.query_records("companies", filter={"domains": domain}, limit=1)
        return results[0] if results else None

    # ═══════════════════════════════════════════════════════════
    # CONVENIENCE — PEOPLE
    # ═══════════════════════════════════════════════════════════

    def upsert_person(self, email: str, values: Optional[dict] = None) -> Optional[dict]:
        """Upsert a person by email address."""
        payload = {"email_addresses": [{"email_address": email}]}
        if values:
            payload.update(values)
        return self.upsert_record("people", "email_addresses", payload)

    def upsert_person_by_linkedin(self, linkedin_url: str, values: Optional[dict] = None) -> Optional[dict]:
        """Upsert a person by LinkedIn URL."""
        payload = {"linkedin_url": linkedin_url}
        if values:
            payload.update(values)
        return self.upsert_record("people", "linkedin_url", payload)

    # ═══════════════════════════════════════════════════════════
    # CONVENIENCE — ROLES
    # ═══════════════════════════════════════════════════════════

    def create_role(self, values: dict) -> Optional[dict]:
        """Create a new role record."""
        return self.create_record("roles", values)

    def update_role(self, record_id: str, values: dict) -> Optional[dict]:
        """Update a role's attributes."""
        return self.update_record("roles", record_id, values)

    def query_roles(
        self,
        filter: Optional[dict] = None,
        sorts: Optional[list] = None,
        limit: int = 50,
    ) -> Optional[list]:
        """Query roles with optional filter."""
        return self.query_records("roles", filter=filter, sorts=sorts, limit=limit)

    # ═══════════════════════════════════════════════════════════
    # LISTS & ENTRIES
    # ═══════════════════════════════════════════════════════════

    def list_lists(self) -> Optional[list]:
        """List all lists in the workspace."""
        resp = self._request("GET", "/lists")
        return resp.get("data") if resp else None

    def create_list(self, name: str, parent_object: str) -> Optional[dict]:
        """Create a new list scoped to an object."""
        return self._request("POST", "/lists", json_data={
            "data": {"name": name, "parent_object": parent_object}
        })

    def add_entry_to_list(self, list_slug: str, record_id: str, values: Optional[dict] = None) -> Optional[dict]:
        """Add a record to a list as an entry."""
        payload = {"data": {"parent_record": record_id}}
        if values:
            payload["data"]["attribute_values"] = values
        return self._request("POST", f"/lists/{list_slug}/entries", json_data=payload)

    def query_list_entries(
        self, list_slug: str, filter: Optional[dict] = None,
        sorts: Optional[list] = None, limit: int = 50, offset: int = 0,
    ) -> Optional[list]:
        """Query entries in a list."""
        payload = {"limit": limit, "offset": offset}
        if filter:
            payload["filter"] = filter
        if sorts:
            payload["sorts"] = sorts
        resp = self._request("POST", f"/lists/{list_slug}/entries/query", json_data=payload)
        return resp.get("data") if resp else None

    # ═══════════════════════════════════════════════════════════
    # NOTES (replaces company_dossier + outreach logging)
    # ═══════════════════════════════════════════════════════════

    def create_note(
        self, parent_object: str, parent_record_id: str,
        title: str, content: str, fmt: str = "markdown",
    ) -> Optional[dict]:
        """Create a note on a record's timeline."""
        return self._request("POST", "/notes", json_data={
            "data": {
                "parent_object": parent_object,
                "parent_record_id": parent_record_id,
                "title": title,
                "format": fmt,
                "content": content,
            }
        })

    def list_notes(self, parent_object: str, parent_record_id: str) -> Optional[list]:
        """List notes on a record."""
        resp = self._request("GET", "/notes", params={
            "parent_object": parent_object,
            "parent_record_id": parent_record_id,
        })
        return resp.get("data") if resp else None

    # ═══════════════════════════════════════════════════════════
    # TASKS
    # ═══════════════════════════════════════════════════════════

    def create_task(
        self, content: str, deadline: Optional[str] = None,
        assignees: Optional[list] = None, linked_records: Optional[list] = None,
    ) -> Optional[dict]:
        """Create a task in Attio."""
        payload = {"data": {"content": content, "is_completed": False}}
        if deadline:
            payload["data"]["deadline"] = deadline
        if assignees:
            payload["data"]["assignees"] = assignees
        if linked_records:
            payload["data"]["linked_records"] = linked_records
        return self._request("POST", "/tasks", json_data=payload)

    # ═══════════════════════════════════════════════════════════
    # WEBHOOKS (optional)
    # ═══════════════════════════════════════════════════════════

    def list_webhooks(self) -> Optional[list]:
        resp = self._request("GET", "/webhooks")
        return resp.get("data") if resp else None

    def create_webhook(self, target_url: str, subscriptions: list) -> Optional[dict]:
        return self._request("POST", "/webhooks", json_data={
            "data": {"target_url": target_url, "subscriptions": subscriptions}
        })

    # ═══════════════════════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════════════════════

    @staticmethod
    def extract_record_id(record: dict) -> Optional[str]:
        """Extract the record ID from an Attio API response record."""
        if not record:
            return None
        rid = record.get("id", {})
        if isinstance(rid, dict):
            return rid.get("record_id")
        return rid

    @staticmethod
    def extract_value(record: dict, attribute_slug: str, default=None):
        """Extract a simple value from a record's values dict."""
        if not record:
            return default
        values = record.get("values", {})
        attr_values = values.get(attribute_slug, [])
        if not attr_values:
            return default
        first = attr_values[0] if isinstance(attr_values, list) else attr_values
        for key in ("value", "domain", "email_address", "original_email_address",
                     "full_name", "first_name", "phone_number", "target_record_id"):
            if key in first:
                return first[key]
        return first

    @staticmethod
    def format_value(value) -> list:
        """Wrap a simple value in Attio's expected format."""
        if isinstance(value, list):
            return value
        return [{"value": value}]

    @staticmethod
    def format_select(option_title: str) -> list:
        """Format a select attribute value."""
        return [{"option": {"title": option_title}}]

    @staticmethod
    def format_record_reference(target_object: str, target_record_id: str) -> list:
        """Format a record-reference attribute value."""
        return [{"target_object": target_object, "target_record_id": target_record_id}]

    def health_check(self) -> bool:
        """Verify API connectivity."""
        try:
            resp = self._request("GET", "/objects", timeout=5)
            return resp is not None
        except Exception:
            return False


# ═══════════════════════════════════════════════════════════
# MODULE-LEVEL SINGLETON
# ═══════════════════════════════════════════════════════════

attio = AttioClient()


# ═══════════════════════════════════════════════════════════
# LOCAL OPS STORE (replaces agent_log, agent_config, apollo_credit_ledger)
# ═══════════════════════════════════════════════════════════

import sqlite3

_OPS_DB_PATH = os.getenv("OPS_DB_PATH", os.path.join(os.path.dirname(__file__), "ops.db"))


def _get_ops_db() -> sqlite3.Connection:
    """Get or create the local ops SQLite database."""
    conn = sqlite3.connect(_OPS_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS agent_config (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS agent_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT,
            entity_type TEXT,
            entity_id TEXT,
            reason TEXT,
            metadata TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS apollo_credit_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT,
            credits INTEGER DEFAULT 0,
            contact_id TEXT,
            company_id TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


def get_config(key: str, default: str = "") -> str:
    conn = _get_ops_db()
    row = conn.execute("SELECT value FROM agent_config WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def set_config(key: str, value: str):
    conn = _get_ops_db()
    conn.execute(
        "INSERT OR REPLACE INTO agent_config (key, value, updated_at) VALUES (?, ?, datetime('now'))",
        (key, value),
    )
    conn.commit()
    conn.close()


def log_action(action: str, entity_type: str = "", entity_id: str = "", reason: str = "", metadata: dict = None):
    conn = _get_ops_db()
    conn.execute(
        "INSERT INTO agent_log (action, entity_type, entity_id, reason, metadata) VALUES (?, ?, ?, ?, ?)",
        (action, entity_type, entity_id, reason, json.dumps(metadata) if metadata else None),
    )
    conn.commit()
    conn.close()


def log_apollo_credits(action: str, credits: int, contact_id: str = "", company_id: str = ""):
    conn = _get_ops_db()
    conn.execute(
        "INSERT INTO apollo_credit_ledger (action, credits, contact_id, company_id) VALUES (?, ?, ?, ?)",
        (action, credits, contact_id, company_id),
    )
    conn.commit()
    conn.close()


def get_apollo_credits_used(since_date: Optional[str] = None) -> int:
    conn = _get_ops_db()
    if since_date:
        row = conn.execute(
            "SELECT COALESCE(SUM(credits), 0) as total FROM apollo_credit_ledger WHERE created_at >= ?",
            (since_date,),
        ).fetchone()
    else:
        row = conn.execute("SELECT COALESCE(SUM(credits), 0) as total FROM apollo_credit_ledger").fetchone()
    conn.close()
    return row["total"] if row else 0
