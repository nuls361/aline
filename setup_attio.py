#!/usr/bin/env python3
"""
Attio Workspace Setup — Creates custom objects, attributes, and pipelines.

Run once to bootstrap your Attio workspace with the full Arteq data model.
Idempotent: checks for existing attributes before creating.

Usage:
    ATTIO_API_KEY=xxx python3 setup_attio.py
"""

import logging
import sys
import time

from attio_client import AttioClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("setup_attio")


# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════

def get_existing_attributes(client: AttioClient, target: str, identifier: str) -> set:
    attrs = client.list_attributes(target, identifier)
    if not attrs:
        return set()
    return {a.get("api_slug") for a in attrs if a.get("api_slug")}


def safe_create_attribute(client, existing, target, identifier, **kwargs):
    slug = kwargs.get("api_slug", "")
    if slug in existing:
        logger.info(f"  ✓ {slug} exists")
        return
    # Ensure description is always set
    if "description" not in kwargs or not kwargs["description"]:
        kwargs["description"] = kwargs.get("title", slug)
    result = client.create_attribute(target, identifier, **kwargs)
    if result:
        logger.info(f"  + {slug}")
    else:
        logger.warning(f"  ✗ {slug} FAILED")
    time.sleep(0.05)


def safe_create_select(client, existing, target, identifier, **kwargs):
    slug = kwargs.get("api_slug", "")
    if slug in existing:
        logger.info(f"  ✓ {slug} exists")
        return
    if "description" not in kwargs or not kwargs["description"]:
        kwargs["description"] = kwargs.get("title", slug)
    result = client.create_select_attribute(target, identifier, **kwargs)
    if result:
        logger.info(f"  + {slug}")
    else:
        logger.warning(f"  ✗ {slug} FAILED")
    time.sleep(0.05)


def safe_create_status(client, existing, target, identifier, title, api_slug, stages, description=""):
    if api_slug in existing:
        logger.info(f"  ✓ {api_slug} exists")
        return
    result = client.create_attribute(
        target, identifier,
        title=title,
        api_slug=api_slug,
        description=description or f"{title} pipeline",
        attr_type="status",
        config={"statuses": [{"title": s} for s in stages]},
    )
    if result:
        logger.info(f"  + {api_slug} ({len(stages)} stages)")
    else:
        logger.warning(f"  ✗ {api_slug} FAILED")
    time.sleep(0.05)


# ═══════════════════════════════════════════════════════════
# COMPANIES — 4 Custom Attributes
# ═══════════════════════════════════════════════════════════

def setup_companies(client):
    logger.info("═══ Companies ═══")
    existing = get_existing_attributes(client, "objects", "companies")

    safe_create_select(client, existing, "objects", "companies",
        title="Company Type", api_slug="company_type",
        description="Demand vs Supply classification",
        options=["prospect", "client", "agency"])

    safe_create_select(client, existing, "objects", "companies",
        title="Specialization", api_slug="specialization",
        description="Agency: which executive functions they cover",
        options=["CFO", "CTO", "COO", "CHRO", "CPO", "CMO", "MD", "Other"],
        is_multiselect=True)

    safe_create_attribute(client, existing, "objects", "companies",
        title="Partner Since", api_slug="partner_since",
        description="Agency: partnership start date",
        attr_type="date")

    # NOTE: Status attributes are only allowed on custom objects and lists,
    # not on standard objects like Companies. Using select instead.
    safe_create_select(client, existing, "objects", "companies",
        title="Agency Stage", api_slug="agency_stage",
        description="Agency acquisition pipeline",
        options=["new", "contacted", "replied", "meeting", "partner", "inactive"])

    logger.info("Done.\n")


# ═══════════════════════════════════════════════════════════
# PEOPLE — 5 Custom Attributes
# ═══════════════════════════════════════════════════════════

def setup_people(client):
    logger.info("═══ People ═══")
    existing = get_existing_attributes(client, "objects", "people")

    safe_create_select(client, existing, "objects", "people",
        title="Person Type", api_slug="person_type",
        description="Role of this person in our business",
        options=["hiring_manager", "candidate", "agency_contact", "other"])

    safe_create_select(client, existing, "objects", "people",
        title="Function", api_slug="function",
        description="Executive function",
        options=["CFO", "CTO", "COO", "CHRO", "CPO", "CMO", "MD", "Other"])

    safe_create_attribute(client, existing, "objects", "people",
        title="Candidate Score", api_slug="candidate_score",
        description="Candidate quality score 0-100",
        attr_type="number")

    safe_create_attribute(client, existing, "objects", "people",
        title="Available", api_slug="available",
        description="Is this candidate currently available",
        attr_type="checkbox")

    # NOTE: Status attributes are only allowed on custom objects and lists,
    # not on standard objects like People. Using select instead.
    safe_create_select(client, existing, "objects", "people",
        title="Candidate Stage", api_slug="candidate_stage",
        description="Candidate sourcing pipeline",
        options=["identified", "contacted", "replied", "meeting",
                 "in_pool", "placed", "inactive"])

    logger.info("Done.\n")


# ═══════════════════════════════════════════════════════════
# ROLE — Custom Object, 18 Attributes
# ═══════════════════════════════════════════════════════════

def setup_role_object(client):
    logger.info("═══ Role Object ═══")

    # Check if Role object exists
    objects = client.list_objects()
    role_exists = any(o.get("api_slug") == "roles" for o in (objects or []))

    if not role_exists:
        result = client.create_object("roles", "Role", "Roles")
        if result:
            logger.info("  + Created Role object")
        else:
            logger.error("  ✗ Failed to create Role object")
            return
        time.sleep(0.5)
    else:
        logger.info("  ✓ Role object exists")

    existing = get_existing_attributes(client, "objects", "roles")

    # Relationships
    safe_create_attribute(client, existing, "objects", "roles",
        title="Company", api_slug="company",
        description="Client company for this role",
        attr_type="record-reference",
        relationship={"object": "companies", "title": "Roles", "api_slug": "roles", "is_multiselect": False})

    safe_create_attribute(client, existing, "objects", "roles",
        title="Contact", api_slug="contact",
        description="Hiring manager at the company",
        attr_type="record-reference",
        relationship={"object": "people", "title": "Roles (Contact)", "api_slug": "roles_contact", "is_multiselect": False})

    safe_create_attribute(client, existing, "objects", "roles",
        title="Candidates", api_slug="candidates",
        description="Matched candidates for this role",
        attr_type="record-reference",
        is_multiselect=True,
        relationship={"object": "people", "title": "Roles (Candidate)", "api_slug": "roles_candidate", "is_multiselect": True})

    # Role details
    safe_create_attribute(client, existing, "objects", "roles",
        title="Title", api_slug="title",
        description="Role title e.g. Interim CFO",
        attr_type="text")

    safe_create_attribute(client, existing, "objects", "roles",
        title="Description", api_slug="description",
        description="Full job description",
        attr_type="text")

    safe_create_attribute(client, existing, "objects", "roles",
        title="Location", api_slug="location",
        description="Role location",
        attr_type="text")

    safe_create_attribute(client, existing, "objects", "roles",
        title="Is Remote", api_slug="is_remote",
        description="Whether the role is remote",
        attr_type="checkbox")

    safe_create_select(client, existing, "objects", "roles",
        title="Source", api_slug="source",
        description="Where this role was found",
        options=["jsearch", "arbeitnow", "linkedin", "manual"])

    safe_create_attribute(client, existing, "objects", "roles",
        title="Source URL", api_slug="source_url",
        description="Link to original job posting",
        attr_type="text")

    safe_create_attribute(client, existing, "objects", "roles",
        title="Posted At", api_slug="posted_at",
        description="When the role was posted",
        attr_type="date")

    # Classification
    safe_create_select(client, existing, "objects", "roles",
        title="Engagement Type", api_slug="engagement_type",
        description="Type of engagement",
        options=["Interim", "Fractional", "Full-time"])

    safe_create_select(client, existing, "objects", "roles",
        title="Role Function", api_slug="role_function",
        description="Executive function of the role",
        options=["Finance", "Engineering", "People", "Operations",
                 "Sales", "Marketing", "Product", "General Management", "Other"])

    safe_create_select(client, existing, "objects", "roles",
        title="Role Level", api_slug="role_level",
        description="Seniority level",
        options=["C-Level", "VP", "Head/Director", "Other"])

    safe_create_attribute(client, existing, "objects", "roles",
        title="Estimated Revenue", api_slug="estimated_revenue",
        description="Estimated revenue from this placement",
        attr_type="currency",
        config={"currency": {"default_currency_code": "EUR", "display_type": "symbol"}})

    # Matching criteria
    safe_create_attribute(client, existing, "objects", "roles",
        title="Must Have Criteria", api_slug="must_have_criteria",
        description="Required criteria for candidate matching",
        attr_type="text")

    safe_create_attribute(client, existing, "objects", "roles",
        title="Nice To Have Criteria", api_slug="nice_to_have_criteria",
        description="Preferred criteria for candidate matching",
        attr_type="text")

    # Pipelines
    safe_create_status(client, existing, "objects", "roles",
        title="Sales Stage", api_slug="sales_stage",
        description="Demand-side sales pipeline",
        stages=["new", "enriching", "ready_for_outreach", "sdr_contacted",
                "replied", "qualified", "meeting", "proposal",
                "closed_won", "closed_lost"])

    safe_create_status(client, existing, "objects", "roles",
        title="Fulfillment Stage", api_slug="fulfillment_stage",
        description="Supply-side fulfillment pipeline",
        stages=["open", "sourcing", "candidates_matched",
                "candidates_presented", "interviews", "offer",
                "placed", "unfilled"])

    logger.info("Done.\n")


# ═══════════════════════════════════════════════════════════
# VERIFICATION
# ═══════════════════════════════════════════════════════════

def verify_setup(client):
    logger.info("═══ Verification ═══")
    for obj in ["companies", "people", "roles"]:
        attrs = client.list_attributes("objects", obj)
        if attrs:
            custom = [a for a in attrs if not a.get("is_system_attribute", False)]
            logger.info(f"  {obj}: {len(attrs)} total, {len(custom)} custom")
        else:
            logger.warning(f"  {obj}: could not list")

    if client.health_check():
        logger.info("  API: OK")
    else:
        logger.error("  API: FAILED")


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    client = AttioClient()

    if not client.api_key:
        logger.error("ATTIO_API_KEY not set.")
        sys.exit(1)

    if not client.health_check():
        logger.error("Cannot reach Attio API.")
        sys.exit(1)

    logger.info("Connected.\n")

    setup_companies(client)
    setup_people(client)
    setup_role_object(client)
    verify_setup(client)

    logger.info("\n✓ Done. Next:")
    logger.info("  1. Check Attio UI")
    logger.info("  2. Create Kanban views: Sales, Fulfillment, Agency, Candidate")
    logger.info("  3. Register agents as members (sdr@arteq.app, ae@arteq.app)")


if __name__ == "__main__":
    main()
