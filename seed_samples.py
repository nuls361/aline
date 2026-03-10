#!/usr/bin/env python3
"""
Seed sample records into Attio for testing.
Creates 5 Companies, 5 People, 5 Roles with realistic data.

Usage:
    ATTIO_API_KEY=xxx python3 seed_samples.py
"""

import logging
import sys
import time
import json

from attio_client import AttioClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_samples")


def main():
    client = AttioClient()

    if not client.api_key:
        logger.error("ATTIO_API_KEY not set.")
        sys.exit(1)

    if not client.health_check():
        logger.error("Cannot reach Attio API.")
        sys.exit(1)

    logger.info("Connected.\n")

    # ═══════════════════════════════════════════════════════════
    # COMPANIES — 5 Samples
    # ═══════════════════════════════════════════════════════════
    logger.info("═══ Companies ═══")

    companies = [
        {
            "name": "TechVenture GmbH",
            "domain": "techventure.de",
            "company_type": "prospect",
        },
        {
            "name": "ScaleUp AG",
            "domain": "scaleup.io",
            "company_type": "prospect",
        },
        {
            "name": "MedTech Solutions",
            "domain": "medtech-solutions.com",
            "company_type": "client",
        },
        {
            "name": "Interim Partners GmbH",
            "domain": "interimpartners.de",
            "company_type": "agency",
            "specialization": "CFO",
            "agency_stage": "partner",
        },
        {
            "name": "Executive Bridge",
            "domain": "executivebridge.com",
            "company_type": "agency",
            "specialization": "CTO",
            "agency_stage": "new",
        },
    ]

    company_ids = {}
    for c in companies:
        domain = c.pop("domain")
        name = c.pop("name")
        company_type = c.pop("company_type")
        specialization = c.pop("specialization", None)
        agency_stage = c.pop("agency_stage", None)

        values = {
            "name": [{"value": name}],
            "domains": [{"domain": domain}],
            "company_type": [{"option": company_type}],
        }
        if specialization:
            values["specialization"] = [{"option": specialization}]
        if agency_stage:
            values["agency_stage"] = [{"option": agency_stage}]

        resp = client.upsert_record("companies", "domains", values)
        if resp:
            rid = resp.get("data", {}).get("id", {}).get("record_id", "?")
            company_ids[domain] = rid
            logger.info(f"  + {name} ({company_type}) → {rid}")
        else:
            logger.warning(f"  ✗ {name} FAILED")
        time.sleep(0.1)

    logger.info("")

    # ═══════════════════════════════════════════════════════════
    # PEOPLE — 5 Samples
    # ═══════════════════════════════════════════════════════════
    logger.info("═══ People ═══")

    people = [
        {
            "name": "Thomas Müller",
            "email": "t.mueller@techventure.de",
            "person_type": "hiring_manager",
            "function": "CFO",
        },
        {
            "name": "Sarah Klein",
            "email": "s.klein@scaleup.io",
            "person_type": "hiring_manager",
            "function": "CTO",
        },
        {
            "name": "Michael Weber",
            "email": "m.weber@gmail.com",
            "person_type": "candidate",
            "function": "CFO",
            "candidate_score": 85,
            "available": True,
            "candidate_stage": "in_pool",
        },
        {
            "name": "Julia Hoffmann",
            "email": "j.hoffmann@outlook.com",
            "person_type": "candidate",
            "function": "CTO",
            "candidate_score": 72,
            "available": True,
            "candidate_stage": "identified",
        },
        {
            "name": "Klaus Richter",
            "email": "k.richter@interimpartners.de",
            "person_type": "agency_contact",
            "function": "Other",
        },
    ]

    people_ids = {}
    for p in people:
        name = p.pop("name")
        email = p.pop("email")
        person_type = p.pop("person_type")
        function = p.pop("function")
        candidate_score = p.pop("candidate_score", None)
        available = p.pop("available", None)
        candidate_stage = p.pop("candidate_stage", None)

        # Split name
        parts = name.split(" ", 1)
        first_name = parts[0]
        last_name = parts[1] if len(parts) > 1 else ""

        values = {
            "name": [{"first_name": first_name, "last_name": last_name, "full_name": name}],
            "email_addresses": [{"email_address": email}],
            "person_type": [{"option": person_type}],
            "function": [{"option": function}],
        }
        if candidate_score is not None:
            values["candidate_score"] = [{"value": candidate_score}]
        if available is not None:
            values["available"] = [{"value": available}]
        if candidate_stage:
            values["candidate_stage"] = [{"option": candidate_stage}]

        resp = client.upsert_record("people", "email_addresses", values)
        if resp:
            rid = resp.get("data", {}).get("id", {}).get("record_id", "?")
            people_ids[email] = rid
            logger.info(f"  + {name} ({person_type}) → {rid}")
        else:
            logger.warning(f"  ✗ {name} FAILED")
        time.sleep(0.1)

    logger.info("")

    # ═══════════════════════════════════════════════════════════
    # ROLES — 5 Samples
    # ═══════════════════════════════════════════════════════════
    logger.info("═══ Roles ═══")

    roles = [
        {
            "title": "Interim CFO",
            "company_domain": "techventure.de",
            "contact_email": "t.mueller@techventure.de",
            "engagement_type": "Interim",
            "role_function": "Finance",
            "role_level": "C-Level",
            "location": "München",
            "is_remote": False,
            "source": "jsearch",
            "sales_stage": "new",
            "fulfillment_stage": "open",
            "estimated_revenue": 45000,
        },
        {
            "title": "Fractional CTO",
            "company_domain": "scaleup.io",
            "contact_email": "s.klein@scaleup.io",
            "engagement_type": "Fractional",
            "role_function": "Engineering",
            "role_level": "C-Level",
            "location": "Berlin",
            "is_remote": True,
            "source": "arbeitnow",
            "sales_stage": "qualified",
            "fulfillment_stage": "sourcing",
            "estimated_revenue": 60000,
        },
        {
            "title": "Interim COO",
            "company_domain": "medtech-solutions.com",
            "contact_email": None,
            "engagement_type": "Interim",
            "role_function": "Operations",
            "role_level": "C-Level",
            "location": "Hamburg",
            "is_remote": False,
            "source": "linkedin",
            "sales_stage": "sdr_contacted",
            "fulfillment_stage": "open",
            "estimated_revenue": 55000,
        },
        {
            "title": "VP Finance",
            "company_domain": "techventure.de",
            "contact_email": "t.mueller@techventure.de",
            "engagement_type": "Full-time",
            "role_function": "Finance",
            "role_level": "VP",
            "location": "München",
            "is_remote": False,
            "source": "jsearch",
            "sales_stage": "meeting",
            "fulfillment_stage": "candidates_matched",
            "estimated_revenue": 30000,
        },
        {
            "title": "Head of People",
            "company_domain": "scaleup.io",
            "contact_email": "s.klein@scaleup.io",
            "engagement_type": "Interim",
            "role_function": "People",
            "role_level": "Head/Director",
            "location": "Remote",
            "is_remote": True,
            "source": "manual",
            "sales_stage": "enriching",
            "fulfillment_stage": "open",
            "estimated_revenue": 35000,
        },
    ]

    for r in roles:
        title = r["title"]
        company_id = company_ids.get(r.pop("company_domain"))
        contact_id = people_ids.get(r.pop("contact_email")) if r.get("contact_email") else None
        r.pop("contact_email", None)

        values = {
            "title": [{"value": r["title"]}],
            "engagement_type": [{"option": r["engagement_type"]}],
            "role_function": [{"option": r["role_function"]}],
            "role_level": [{"option": r["role_level"]}],
            "location": [{"value": r["location"]}],
            "is_remote": [{"value": r["is_remote"]}],
            "source": [{"option": r["source"]}],
            "sales_stage": [{"status": r["sales_stage"]}],
            "fulfillment_stage": [{"status": r["fulfillment_stage"]}],
            "estimated_revenue": [{"currency_value": r["estimated_revenue"]}],
        }

        if company_id:
            values["company"] = [{"target_object": "companies", "target_record_id": company_id}]
        if contact_id:
            values["contact"] = [{"target_object": "people", "target_record_id": contact_id}]

        resp = client.create_record("roles", values)
        if resp:
            rid = resp.get("data", {}).get("id", {}).get("record_id", "?")
            logger.info(f"  + {title} → {rid}")
        else:
            logger.warning(f"  ✗ {title} FAILED")
        time.sleep(0.1)

    logger.info("\n✓ Done. Check Attio UI.")


if __name__ == "__main__":
    main()
