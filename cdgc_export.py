"""
cdgc_export.py

Exports all CDGC Catalog Sources to individual JSON files (with system/read-only
fields stripped), and builds role + group lookup tables as JSON files.

Output structure:
  ./export/
    catalog_sources/
      <catalog_source_name>.json   (one file per source, clean payload)
    roles_lookup.json              (id -> roleName mapping)
    groups_lookup.json             (id -> userGroupName mapping)

Usage:
  python cdgc_export.py

Configure the constants below, or set environment variables:
  IDMC_BASE_URL   e.g. https://dm-us.informaticacloud.com
  IDMC_USERNAME
  IDMC_PASSWORD

Org ID is extracted automatically from the login response (currentOrgId) --
no need to configure it separately.
"""

import os
import csv
import json
import re
import datetime
import requests

# ---------------------------------------------------------------------------
# Configuration -- edit here or set environment variables
# ---------------------------------------------------------------------------
# Pod identifier — the regional segment of the Informatica Cloud URLs
# e.g. "dm-us", "dm-eu", "dm-ap", etc.
POD        = os.getenv("IDMC_POD",      "dmp-us")

IDMC_URL   = f"https://{POD}.informaticacloud.com"        # {{url}} in Postman
API_URL    = f"https://idmc-api.{POD}.informaticacloud.com"  # {{api.url}} in Postman

USERNAME   = os.getenv("IDMC_USERNAME", "shayes_compass")
PASSWORD   = os.getenv("IDMC_PASSWORD", "Infa2025c!")

OUTPUT_DIR   = "./export"
CS_DIR       = os.path.join(OUTPUT_DIR, "catalog_sources")
CONN_DIR     = os.path.join(OUTPUT_DIR, "connections")

# Fields returned by GET that the PUT endpoint does not accept
STRIP_FIELDS = {
    "id",
    "endOfLife",
    "seedVersion",
    "isDeleted",
    "createdBy",
    "lastModifiedBy",
    "createdTime",
    "lastModifiedTime",
    "modelVersion",
}

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def login(pod_url, username, password):
    """
    Two-step auth matching iicsutils.py exactly:

    Step 1 - POST /saas/public/core/v3/login (v3 login)
      - sessionId at data['userInfo']['sessionId']
      - iics_url from data['products'][0]['baseApiUrl'] with /saas stripped
        e.g. https://usw1.dmp-us.informaticacloud.com/saas -> https://usw1.dmp-us.informaticacloud.com
      - Roles/groups use: iics_url + /saas/public/core/v3/...

    Step 2 - POST /identity-service/api/v1/jwt/Token (on pod_url, not iics_url)
      - Headers: INFA-SESSION-ID / IDS-SESSION-ID / icSessionId = sessionId
      - client_id=cdlg_app, matching iicsutils.py
      - Returns jwt_token used as Bearer for /data360/ catalog source calls
    """
    # --- Step 1: v3 Login ---
    login_url  = f"{pod_url}/saas/public/core/v3/login"
    login_resp = requests.post(
        login_url,
        json={"username": username, "password": password},
        headers={"content-type": "application/json"},
        timeout=30,
    )
    login_resp.raise_for_status()
    login_data = login_resp.json()

    user_info  = login_data.get("userInfo", {})
    session_id = user_info.get("sessionId")
    org_id     = user_info.get("orgId")
    org_name   = user_info.get("orgName", "")

    raw_base_url = login_data.get("products", [{}])[0].get("baseApiUrl", "")
    if raw_base_url.endswith("/saas"):
        iics_url = raw_base_url[:-len("/saas")]
    else:
        iics_url = raw_base_url or pod_url

    if not session_id:
        raise RuntimeError(f"Login step 1 failed: no sessionId. Response: {login_data}")
    if not org_id:
        raise RuntimeError(f"Login step 1 failed: no orgId. Response: {login_data}")

    print(f"  Logged in as : {user_info.get('name', username)}")
    print(f"  Org          : {org_name} ({org_id})")
    print(f"  Session ID   : {session_id[:8]}...")
    print(f"  IICS URL     : {iics_url}")

    # --- Step 2: JWT Token (use pod_url, session headers matching iicsutils.py) ---
    token_url     = f"{pod_url}/identity-service/api/v1/jwt/Token"
    token_headers = {
        "Accept":          "application/json",
        "INFA-SESSION-ID": session_id,
        "IDS-SESSION-ID":  session_id,
        "icSessionId":     session_id,
    }
    token_resp = requests.post(
        token_url,
        params={"client_id": "cdlg_app", "nonce": "g3t69BWB49BHHNn", "access_code": ""},
        headers=token_headers,
        timeout=30,
    )
    token_resp.raise_for_status()
    token_data   = token_resp.json()
    access_token = token_data.get("jwt_token")

    if not access_token:
        raise RuntimeError(f"Login step 2 failed: no jwt_token. Response: {token_data}")

    print(f"  JWT token    : {access_token[:12]}...")

    # api_url: idmc-api subdomain built from pod_url (NOT iics_url)
    # e.g. https://dmp-us.informaticacloud.com -> https://idmc-api.dmp-us.informaticacloud.com
    api_url = pod_url.replace("https://", "https://idmc-api.")

    print(f"  API URL      : {api_url}")

    return {
        "session_id":   session_id,   # for /saas/public/core/v3/ calls (roles, groups)
        "access_token": access_token, # for /data360/ calls (catalog sources)
        "org_id":       org_id,
        "org_name":     org_name,
        "iics_url":     iics_url,     # usw1.dmp-us... (from login response) for roles/groups
        "api_url":      api_url,      # idmc-api.dmp-us... (from pod_url) for data360
    }


def cdgc_headers(auth):
    """Headers for CDGC catalog-source-management API calls."""
    return {
        "X-INFA-ORG-ID":   auth["org_id"],
        "IDS-SESSION-ID":  auth["session_id"],
        "Authorization":   f"Bearer {auth['access_token']}",
        "Content-Type":    "application/json",
    }


def idmc_headers(auth):
    """Headers for IDMC /saas/public/core/v3/ calls (roles, groups).
    These endpoints use the sessionId directly, not the JWT Bearer token."""
    return {
        "INFA-SESSION-ID": auth["session_id"],
        "IDS-SESSION-ID":  auth["session_id"],
        "icSessionId":     auth["session_id"],
        "Content-Type":    "application/json",
        "Accept":          "application/json",
    }


# ---------------------------------------------------------------------------
# Catalog Sources
# ---------------------------------------------------------------------------

def fetch_all_catalog_sources(api_url, auth):
    """Pages through all catalog sources and returns the full list."""
    endpoint = f"{api_url}/data360/catalog-source-management/v1/catalogsources"
    offset   = 0
    limit    = 50
    results  = []

    while True:
        params = {"offset": offset, "limit": limit, "sort": "name:ASC"}
        resp   = requests.get(endpoint, headers=cdgc_headers(auth), params=params, timeout=30)
        resp.raise_for_status()
        data  = resp.json()

        # Response is either a plain list or a wrapper with a catalogSources/items key
        batch = data if isinstance(data, list) else (data.get("catalogSources") or data.get("items") or [])
        results.extend(batch)
        print(f"    Fetched {len(results)} catalog source(s) so far...")

        if len(batch) < limit:
            break
        offset += limit

    return results


def strip_fields(source):
    return {k: v for k, v in source.items() if k not in STRIP_FIELDS}


def safe_filename(name):
    """Converts a catalog source name into a safe filename."""
    return re.sub(r'[\\/*?:"<>|]', "_", name)


def export_catalog_sources(auth):
    os.makedirs(CS_DIR, exist_ok=True)
    print("\n[1/6] Fetching catalog sources...")
    sources = fetch_all_catalog_sources(auth["api_url"], auth)

    for source in sources:
        name  = source.get("name", source.get("id", "unknown"))
        clean = strip_fields(source)
        fpath = os.path.join(CS_DIR, safe_filename(name) + ".json")
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(clean, f, indent=4)

    print(f"  Saved {len(sources)} catalog source(s) to {CS_DIR}/")
    return len(sources)


# ---------------------------------------------------------------------------
# Roles lookup
# ---------------------------------------------------------------------------

def export_roles(auth):
    """Fetches all roles and saves a lookup: { id: roleName, ... }"""
    print("\n[2/6] Fetching roles...")
    idmc_url = auth["iics_url"]
    endpoint = f"{idmc_url}/saas/public/core/v3/roles"
    offset   = 0
    limit    = 200
    lookup   = {}

    while True:
        params = {"limit": limit, "skip": offset}
        resp   = requests.get(endpoint, headers=idmc_headers(auth), params=params, timeout=30)
        resp.raise_for_status()
        data  = resp.json()
        roles = data if isinstance(data, list) else data.get("roles", [])

        for role in roles:
            role_id = role.get("id")
            if role_id:
                lookup[role_id] = role.get("roleName") or role.get("name")

        print(f"    Fetched {len(lookup)} role(s) so far...")
        if len(roles) < limit:
            break
        offset += limit

    out_path = os.path.join(OUTPUT_DIR, "roles_lookup.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(lookup, f, indent=4)

    print(f"  Saved {len(lookup)} role(s) to {out_path}")
    return lookup


# ---------------------------------------------------------------------------
# Groups lookup
# ---------------------------------------------------------------------------

def export_groups(auth):
    """Fetches all user groups and saves a lookup: { id: userGroupName, ... }"""
    print("\n[3/6] Fetching user groups...")
    idmc_url = auth["iics_url"]
    endpoint = f"{idmc_url}/saas/public/core/v3/userGroups"
    offset   = 0
    limit    = 200
    lookup   = {}

    while True:
        params = {"limit": limit, "skip": offset}
        resp   = requests.get(endpoint, headers=idmc_headers(auth), params=params, timeout=30)
        resp.raise_for_status()
        data   = resp.json()
        groups = data if isinstance(data, list) else data.get("userGroups", [])

        for group in groups:
            group_id = group.get("id")
            if group_id:
                lookup[group_id] = group.get("userGroupName") or group.get("name")

        print(f"    Fetched {len(lookup)} group(s) so far...")
        if len(groups) < limit:
            break
        offset += limit

    out_path = os.path.join(OUTPUT_DIR, "groups_lookup.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(lookup, f, indent=4)

    print(f"  Saved {len(lookup)} group(s) to {out_path}")
    return lookup


# ---------------------------------------------------------------------------
# Runtime Environments lookup
# ---------------------------------------------------------------------------

def export_runtime_environments(auth):
    """Fetches all runtime environments and saves a lookup: { name: id, ... }
    Matches iicsutils.getRunTimeEnvironmentsDict() exactly.
    Endpoint: iics_url + /saas/api/v2/runtimeEnvironment
    """
    print("\n[4/6] Fetching runtime environments...")
    endpoint = f"{auth['iics_url']}/saas/api/v2/runtimeEnvironment"
    resp = requests.get(endpoint, headers=idmc_headers(auth), timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Returns a plain list of environment objects
    lookup = {}
    for env in (data if isinstance(data, list) else []):
        name = env.get("name")
        eid  = env.get("id")
        if name and eid:
            lookup[name] = eid

    out_path = os.path.join(OUTPUT_DIR, "runtime_environments_lookup.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(lookup, f, indent=4)

    print(f"  Saved {len(lookup)} runtime environment(s) to {out_path}")
    return lookup


# ---------------------------------------------------------------------------
# Connections lookup
# ---------------------------------------------------------------------------

MASKED_VALUE = "********"


def find_encrypted_fields(obj, prefix=""):
    """
    Recursively scan a connection object for fields whose value is the
    Informatica masked sentinel '********'. Returns a list of dot-notation
    field paths, e.g. ['password', 'connParams.privateKey'].
    """
    found = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else k
            if v == MASKED_VALUE:
                found.append(path)
            else:
                found.extend(find_encrypted_fields(v, path))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            found.extend(find_encrypted_fields(v, f"{prefix}[{i}]"))
    return found


def export_encrypted_fields_csv(conn_dir, output_dir):
    """
    Scans all exported connection JSON files for masked ('********') fields
    and writes a template CSV the user can fill in before importing.

    Output: export/encrypted_fields_<timestamp>.csv
    Columns: Connection Name, Parameter Name, Value
    """
    rows = []
    for fname in sorted(os.listdir(conn_dir)):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(conn_dir, fname)
        with open(fpath, encoding="utf-8") as f:
            conn = json.load(f)
        name = conn.get("name", fname[:-5])
        for field_path in find_encrypted_fields(conn):
            rows.append((name, field_path))

    if not rows:
        print("  No encrypted fields found — CSV not created.")
        return None

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(output_dir, f"encrypted_fields_{timestamp}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Connection Name", "Parameter Name", "Value"])
        for conn_name, field_path in rows:
            writer.writerow([conn_name, field_path, ""])

    uniq = len({r[0] for r in rows})
    print(f"  Wrote encrypted fields template: {csv_path}")
    print(f"  ({len(rows)} field(s) across {uniq} connection(s) — fill in Value column before importing)")
    return csv_path


def export_connections(auth):
    """Fetches all connections and:
      - Saves each connection as an individual JSON file in connections/
      - Saves a name->id lookup as connections_lookup.json

    The list endpoint (/saas/api/v2/connection) returns lightweight objects
    that omit connector-specific connParams (e.g. Snowflake account, warehouse).
    We therefore fetch each connection individually by ID using
    /saas/api/v2/connection/{id} to get the full payload.
    """
    print("\n[5/6] Fetching connections...")
    os.makedirs(CONN_DIR, exist_ok=True)

    # Step 1: Get the list to discover all connection IDs
    list_endpoint = f"{auth['iics_url']}/saas/api/v2/connection"
    resp = requests.get(list_endpoint, headers=idmc_headers(auth), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    connections_list = data if isinstance(data, list) else []
    print(f"  Found {len(connections_list)} connection(s) — fetching full details per connection...")

    lookup = {}
    saved = 0

    for conn_stub in connections_list:
        name = conn_stub.get("name")
        cid  = conn_stub.get("id")
        if not cid:
            print(f"  WARNING: Connection with no ID skipped: {conn_stub}")
            continue

        if name and cid:
            lookup[name] = cid

        # Step 2: Fetch the full connection object by ID
        detail_endpoint = f"{auth['iics_url']}/saas/api/v2/connection/{cid}"
        detail_resp = requests.get(detail_endpoint, headers=idmc_headers(auth), timeout=30)
        if detail_resp.status_code == 200:
            conn_full = detail_resp.json()
            # The detail endpoint may return a list or a single object
            if isinstance(conn_full, list):
                conn_full = conn_full[0] if conn_full else conn_stub
        else:
            print(f"  WARNING: Could not fetch detail for '{name}' ({cid}): "
                  f"{detail_resp.status_code} — using list-level data")
            conn_full = conn_stub

        fname = safe_filename(name or cid) + ".json"
        fpath = os.path.join(CONN_DIR, fname)
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(conn_full, f, indent=4)
        saved += 1

    out_path = os.path.join(OUTPUT_DIR, "connections_lookup.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(lookup, f, indent=4)

    print(f"  Saved {saved} connection file(s) to {CONN_DIR}/")
    print(f"  Saved {len(lookup)} connection(s) to {out_path}")

    print("\n  Scanning for encrypted fields...")
    enc_csv = export_encrypted_fields_csv(CONN_DIR, OUTPUT_DIR)

    return lookup, enc_csv


# ---------------------------------------------------------------------------
# Classifications lookup
# ---------------------------------------------------------------------------

def export_classifications(auth):
    """Fetches all classifications and saves a lookup: { externalId: name, ... }
    Uses the CDGC search API with knowledgeQuery=classifications.
    Paginates using from/size in the POST body.
    """
    print("\n[6/6] Fetching classifications...")
    endpoint = f"{auth['api_url']}/data360/search/v1/assets?knowledgeQuery=classifications&segments=all"
    page_size = 100
    offset    = 0
    lookup    = {}

    while True:
        resp = requests.post(
            endpoint,
            headers=cdgc_headers(auth),
            json={"from": offset, "size": page_size},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        hits = data.get("hits", [])
        for hit in hits:
            external_id = hit.get("core.externalId")
            name        = hit.get("summary", {}).get("core.name")
            if external_id and name:
                lookup[external_id] = name

        total = int(data.get("summary", {}).get("total_hits", 0))
        print(f"    Fetched {len(lookup)} of {total} classification(s)...")

        offset += page_size
        if offset >= total:
            break

    out_path = os.path.join(OUTPUT_DIR, "classifications_lookup.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(lookup, f, indent=4)

    print(f"  Saved {len(lookup)} classification(s) to {out_path}")
    return lookup



# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("CDGC Export Script")
    print("=" * 60)
    print(f"\nPod      : {POD}")
    print(f"IDMC URL : {IDMC_URL}")
    print(f"API URL  : {API_URL}")
    print(f"Output   : {OUTPUT_DIR}/\n")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("[Auth] Logging in...")
    auth = login(IDMC_URL, USERNAME, PASSWORD)  # IDMC_URL = pod_url e.g. https://dmp-us.informaticacloud.com

    cs_count = export_catalog_sources(auth)
    roles    = export_roles(auth)
    groups   = export_groups(auth)
    runtimes        = export_runtime_environments(auth)
    connections, enc_csv = export_connections(auth)
    classifications = export_classifications(auth)

    print("\n" + "=" * 60)
    print("Export complete!")
    print(f"  Catalog sources      : {cs_count} file(s) in {CS_DIR}/")
    print(f"  Roles lookup         : {len(roles)} entries in {OUTPUT_DIR}/roles_lookup.json")
    print(f"  Groups lookup        : {len(groups)} entries in {OUTPUT_DIR}/groups_lookup.json")
    print(f"  Runtime environments : {len(runtimes)} entries in {OUTPUT_DIR}/runtime_environments_lookup.json")
    print(f"  Connections          : {len(connections)} entries in {OUTPUT_DIR}/connections_lookup.json + files in {CONN_DIR}/")
    print(f"  Classifications      : {len(classifications)} entries in {OUTPUT_DIR}/classifications_lookup.json")
    if enc_csv:
        print(f"  Encrypted fields CSV : {enc_csv}")
        print(f"  *** Fill in the Value column before running the import ***")
    print("=" * 60)


if __name__ == "__main__":
    main()
