"""
cdgc_export.py

Exports connections and CDGC catalog sources, then bundles everything into a
single timestamped zip file.  The zip is the sole output artifact — no
persistent export directory is left behind.

Output:
  export_<timestamp>.zip  (written to cdgc_export_zip_dir, default: CWD)

Inside the zip:
  catalog_sources/<name>.json
  catalog_source_zips/<name>.zip
  connections/<name>.json
  roles_lookup.json
  runtime_environments_lookup.json
  connections_lookup.json
  catalog_sources_id_lookup.json
  classifications_lookup.json
  encrypted_fields_empty.csv   (fill in, rename to encrypted_fields.csv, use on import)

Usage:
  python cdgc_export.py [--cdgc_export_pod POD] [--cdgc_export_username USER]
                        [--cdgc_export_password PASS] [--cdgc_export_zip_dir DIR]
                        [--cdgc_export_no_zips] [--cdgc_export_no_connections]
                        [--cdgc_export_no_catalog_sources]
                        [--cdgc_export_filter_connections REGEX]
                        [--cdgc_export_filter_catalog_sources REGEX]

Configure via cdgc_config.ini [config] section, environment variables, or CLI args.
All three mechanisms use the same key names (e.g. cdgc_export_username).
"""

import os
import csv
import json
import re
import datetime
import argparse
import zipfile
import shutil
import tempfile
import configparser
import requests


_INI_FILENAME = "cdgc_config.ini"
_SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
_ini_cache    = None


def _load_ini() -> dict:
    global _ini_cache
    if _ini_cache is not None:
        return _ini_cache
    cp = configparser.ConfigParser(interpolation=None)
    for base in (_SCRIPT_DIR, os.getcwd()):
        path = os.path.join(base, _INI_FILENAME)
        if os.path.isfile(path):
            cp.read(path, encoding="utf-8")
            print(f"  [config] Loaded {path}")
            break
    _ini_cache = dict(cp["config"]) if cp.has_section("config") else {}
    return _ini_cache


def get(full_key: str, default: str = "") -> str:
    ini = _load_ini()
    val = ini.get(full_key.lower(), "").strip()
    if val:
        return val
    for env_key in (full_key, full_key.upper()):
        val = os.environ.get(env_key, "").strip()
        if val:
            return val
    return default


def get_bool(full_key: str, default: bool = True) -> bool:
    val = get(full_key).lower()
    if val in ("true", "1", "yes"):
        return True
    if val in ("false", "0", "no"):
        return False
    return default

# ---------------------------------------------------------------------------
# Configuration — defaults come from INI file, env vars, then hardcoded values
# ---------------------------------------------------------------------------
P = "cdgc_export"   # prefix for all keys in this script

POD      = get(f"{P}_pod",      "dmp-us")
USERNAME = get(f"{P}_username", "your_username_here")
PASSWORD = get(f"{P}_password", "your_password_here")

# Where to write the final zip (default: current working directory)
ZIP_DIR = get(f"{P}_zip_dir", ".")

# Optional explicit temp dir; supports {timestamp}. If blank, OS temp is used.
TEMP_DIR_TEMPLATE = get(f"{P}_temp_dir", "")

# These are set dynamically at runtime from the temp staging dir
OUTPUT_DIR  = None
CS_DIR      = None
CS_ZIPS_DIR = None
CONN_DIR    = None

DOWNLOAD_ZIPS          = get_bool(f"{P}_download_zips",          True)
ENABLE_CONNECTIONS     = get_bool(f"{P}_enable_connections",     True)
ENABLE_CATALOG_SOURCES = get_bool(f"{P}_enable_catalog_sources", True)
FILTER_CONNECTIONS     = get(f"{P}_filter_connections")     or None
FILTER_CATALOG_SOURCES = get(f"{P}_filter_catalog_sources") or None

IDMC_URL = f"https://{POD}.informaticacloud.com"
API_URL  = f"https://idmc-api.{POD}.informaticacloud.com"

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
# Filter helper
# ---------------------------------------------------------------------------

def _name_matches(name, pattern):
    """Returns True if pattern is None/empty, or if name matches the regex."""
    if not pattern:
        return True
    return bool(re.search(pattern, name, re.IGNORECASE))


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
    api_url      = pod_url.replace("https://", "https://idmc-api.")
    cdgc_api_url = pod_url.replace("https://", "https://cdgc-api.")

    print(f"  API URL      : {api_url}")
    print(f"  CDGC API URL : {cdgc_api_url}")

    return {
        "session_id":   session_id,   # for /saas/public/core/v3/ calls (roles, groups)
        "access_token": access_token, # for /data360/ calls (catalog sources)
        "org_id":       org_id,
        "org_name":     org_name,
        "iics_url":     iics_url,     # usw1.dmp-us... (from login response) for roles/groups
        "api_url":      api_url,      # idmc-api.dmp-us... (from pod_url) for data360
        "cdgc_api_url": cdgc_api_url, # cdgc-api.dmp-us... for staging/datasource APIs
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


def normalize_name_for_filename(name):
    """Normalizes a display name into a compact filename-safe token."""
    cleaned = safe_filename(name or "").strip()
    cleaned = re.sub(r"\s+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("._")
    return cleaned.lower() or "unknown_org"


# Keys used (in priority order) to sort arrays of dicts within a catalog source
# payload so that the saved JSON is deterministic across API calls.
_SORT_KEYS = ("capabilityName", "globalConfigurationName", "optionGroupName", "key", "name")


def _sort_cs_payload(obj):
    """
    Recursively sorts arrays of dicts within a catalog source payload by a
    stable identifying key, so that re-exported JSONs are identical regardless
    of the order the API returns sub-arrays in each call.
    """
    if isinstance(obj, dict):
        return {k: _sort_cs_payload(v) for k, v in obj.items()}
    if isinstance(obj, list):
        items = [_sort_cs_payload(v) for v in obj]
        if items and isinstance(items[0], dict):
            for sort_key in _SORT_KEYS:
                if any(sort_key in item for item in items):
                    try:
                        items.sort(key=lambda x: json.dumps(x.get(sort_key, ""), sort_keys=True))
                    except Exception:
                        pass
                    break
        return items
    return obj


def export_catalog_sources(auth):
    os.makedirs(CS_DIR, exist_ok=True)
    print("\n[1/6] Fetching catalog sources...")
    sources = fetch_all_catalog_sources(auth["api_url"], auth)

    # Save name→id lookup before stripping IDs — required by the import to remap
    # catalog source IDs embedded inside custom lineage zip files (links.csv).
    id_lookup = {
        s["name"]: s["id"]
        for s in sources
        if s.get("name") and s.get("id")
    }
    lookup_path = os.path.join(OUTPUT_DIR, "catalog_sources_id_lookup.json")
    with open(lookup_path, "w", encoding="utf-8") as f:
        json.dump(id_lookup, f, indent=4)
    print(f"  Saved catalog source ID lookup ({len(id_lookup)} entries) to {lookup_path}")

    if FILTER_CATALOG_SOURCES:
        before = len(sources)
        sources = [s for s in sources if _name_matches(s.get("name", ""), FILTER_CATALOG_SOURCES)]
        print(f"  Filter '{FILTER_CATALOG_SOURCES}' applied: {len(sources)} of {before} source(s) match")

    for source in sources:
        name  = source.get("name", source.get("id", "unknown"))
        clean = _sort_cs_payload(strip_fields(source))
        fpath = os.path.join(CS_DIR, safe_filename(name) + ".json")
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(clean, f, indent=4)

    print(f"  Saved {len(sources)} catalog source(s) to {CS_DIR}/")
    return len(sources)


def _get_file_details(cs):
    """Returns (filename, file_path_id) from a custom source's File Details, or (None, None)."""
    for group in cs.get("typeOptions", {}).get("configurationProperties", []):
        if group.get("optionGroupName") == "Custom OptionGroup":
            for opt in group.get("configOptions", []):
                if opt.get("key") == "File Details":
                    vals = opt.get("values", [])
                    if len(vals) >= 2:
                        return vals[0], vals[1]
    return None, None


def download_catalog_source_zips(auth):
    """
    For each custom catalog source with File Details, downloads the associated
    zip from the source org's staging storage and saves it to CS_ZIPS_DIR.

    Endpoint (confirmed from browser capture):
      GET /ccgf-metadata-staging/api/v1/staging/files/download
        ?filePath={cs_id}/{cs_id}.zip
        &serviceFunction=custom-metadata-staging-producer

    Returns binary zip content directly (application/octet-stream).
    The file path format after catalog source creation is {cs_id}/{cs_id}.zip
    where cs_id is the catalog source's own UUID.
    """
    if not DOWNLOAD_ZIPS:
        return 0

    os.makedirs(CS_ZIPS_DIR, exist_ok=True)
    print(f"\n[+] Downloading custom catalog source zip files...")

    # Re-fetch live catalog sources so we have their IDs (stripped from saved JSON)
    print("  Fetching live catalog source IDs from API...")
    live_sources = fetch_all_catalog_sources(auth["api_url"], auth)
    cs_name_to_id = {s["name"]: s["id"] for s in live_sources if s.get("name") and s.get("id")}
    print(f"  Found {len(cs_name_to_id)} catalog source IDs.")

    downloaded, skipped, failed = 0, 0, 0
    download_url = f"{auth['cdgc_api_url']}/ccgf-metadata-staging/api/v1/staging/files/download"
    headers = {
        "X-INFA-ORG-ID":  auth["org_id"],
        "IDS-SESSION-ID": auth["session_id"],
        "Authorization":  f"Bearer {auth['access_token']}",
    }

    for fname in sorted(os.listdir(CS_DIR)):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(CS_DIR, fname), encoding="utf-8") as f:
            cs = json.load(f)

        if not cs.get("custom"):
            continue

        cs_name = cs.get("name", fname[:-5])
        if not _name_matches(cs_name, FILTER_CATALOG_SOURCES):
            continue
        filename, file_path_id = _get_file_details(cs)
        if not filename:
            print(f"  SKIP     : {cs_name} — no File Details found")
            skipped += 1
            continue

        cs_id = cs_name_to_id.get(cs_name)
        if not cs_id:
            print(f"  SKIP     : {cs_name} — ID not found in live catalog sources")
            skipped += 1
            continue

        local_zip = os.path.join(CS_ZIPS_DIR, safe_filename(cs_name) + ".zip")
        file_path = f"{cs_id}/{cs_id}.zip"

        resp = requests.get(
            download_url,
            headers=headers,
            params={"filePath": file_path, "serviceFunction": "custom-metadata-staging-producer"},
            timeout=120,
        )

        if resp.ok and resp.content:
            with open(local_zip, "wb") as f:
                f.write(resp.content)
            print(f"  DOWNLOAD : {cs_name}  ({len(resp.content):,} bytes)")
            downloaded += 1
        else:
            print(f"  FAIL     : {cs_name}  — {resp.status_code} {resp.text[:120]}")
            failed += 1

    print(f"  Zip files — Downloaded: {downloaded}  Skipped: {skipped}  Failed: {failed}")
    return downloaded


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


def export_encrypted_fields_csv(conn_dir, cs_dir, output_dir):
    """
    Scans exported connections and catalog sources for encrypted/masked fields
    and writes a single combined template CSV the user can fill in before importing.

    Output: <output_dir>/encrypted_fields_<timestamp>.csv
    Columns: Type, Name, Parameter, Value
      Type = "Connection"       for connection masked fields (******** sentinel)
             "Catalog Resource" for catalog source AAA-encrypted blobs
    """
    rows = []  # list of (type, name, parameter)

    # Connections — masked with ********
    if conn_dir and os.path.isdir(conn_dir):
        for fname in sorted(os.listdir(conn_dir)):
            if not fname.endswith(".json"):
                continue
            with open(os.path.join(conn_dir, fname), encoding="utf-8") as f:
                conn = json.load(f)
            name = conn.get("name", fname[:-5])
            for field_path in find_encrypted_fields(conn):
                rows.append(("Connection", name, field_path))

    # Catalog sources — org-encrypted AAA blobs
    if cs_dir and os.path.isdir(cs_dir):
        for fname in sorted(os.listdir(cs_dir)):
            if not fname.endswith(".json"):
                continue
            with open(os.path.join(cs_dir, fname), encoding="utf-8") as f:
                cs = json.load(f)
            name = cs.get("name", fname[:-5])
            for json_path in find_encrypted_cs_fields(cs):
                rows.append(("Catalog Resource", name, json_path))

    if not rows:
        print("  No encrypted fields found — CSV not created.")
        return None

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(output_dir, f"encrypted_fields_{timestamp}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Type", "Name", "Parameter", "Value"])
        for row_type, name, param in rows:
            writer.writerow([row_type, name, param, ""])

    conn_count = sum(1 for r in rows if r[0] == "Connection")
    res_count  = sum(1 for r in rows if r[0] == "Catalog Resource")
    print(f"  Wrote encrypted fields template: {csv_path}")
    print(f"  ({conn_count} connection field(s), {res_count} catalog resource field(s) — fill in Value column before importing)")
    return csv_path


# Informatica encrypted config values: long base64 blobs starting with 'AAA'
_INFA_ENCRYPTED_RE = re.compile(r'^AAA[A-Za-z0-9+/]{77,}={0,2}$')

# Keys checked (in order) when building a meaningful array-element path identifier
_PATH_ID_KEYS = ("optionGroupName", "capabilityName", "globalConfigurationName", "key", "name")


def find_encrypted_cs_fields(payload):
    """
    Recursively walks a catalog source payload and returns a list of JSONPath-style
    strings for every Informatica-encrypted value (long AAA-prefixed base64 blob), e.g.:

      typeOptions.configurationProperties[optionGroupName="IICS OptionGroup"]
        .configOptions[key="IICS Password"].values[0]
    """
    found = []

    def _walk(obj, path):
        if isinstance(obj, dict):
            for k, v in obj.items():
                _walk(v, f"{path}.{k}" if path else k)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, dict):
                    id_key = next((k for k in _PATH_ID_KEYS if k in item), None)
                    elem_id = f'[{id_key}="{item[id_key]}"]' if id_key else f"[{i}]"
                else:
                    elem_id = f"[{i}]"
                _walk(item, f"{path}{elem_id}")
        elif isinstance(obj, str) and _INFA_ENCRYPTED_RE.match(obj):
            found.append(path)

    _walk(payload, "")
    return found




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
    if FILTER_CONNECTIONS:
        before = len(connections_list)
        connections_list = [c for c in connections_list if _name_matches(c.get("name", ""), FILTER_CONNECTIONS)]
        print(f"  Filter '{FILTER_CONNECTIONS}' applied: {len(connections_list)} of {before} connection(s) match")
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

    return lookup


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
    global POD, USERNAME, PASSWORD, ZIP_DIR, OUTPUT_DIR, CS_DIR, CS_ZIPS_DIR, CONN_DIR
    global DOWNLOAD_ZIPS, ENABLE_CONNECTIONS, ENABLE_CATALOG_SOURCES
    global FILTER_CONNECTIONS, FILTER_CATALOG_SOURCES, IDMC_URL, API_URL

    parser = argparse.ArgumentParser(description="Export CDGC connections and catalog sources")
    parser.add_argument(f"--{P}_pod",                    default=POD,      metavar="POD",  dest="pod")
    parser.add_argument(f"--{P}_username",               default=USERNAME, metavar="USER", dest="username")
    parser.add_argument(f"--{P}_password",               default=PASSWORD, metavar="PASS", dest="password")
    parser.add_argument(f"--{P}_zip_dir",                default=ZIP_DIR,  metavar="DIR",  dest="zip_dir",
                        help="Directory to write the output zip (default: current directory)")
    parser.add_argument(f"--{P}_no_zips",                action="store_true", dest="no_zips",
                        help="Skip downloading custom source zip files")
    parser.add_argument(f"--{P}_no_connections",         action="store_true", dest="no_connections")
    parser.add_argument(f"--{P}_no_catalog_sources",     action="store_true", dest="no_catalog_sources")
    parser.add_argument(f"--{P}_filter_connections",     default=FILTER_CONNECTIONS,     metavar="REGEX", dest="filter_connections")
    parser.add_argument(f"--{P}_filter_catalog_sources", default=FILTER_CATALOG_SOURCES, metavar="REGEX", dest="filter_catalog_sources")
    args = parser.parse_args()

    POD      = args.pod
    USERNAME = args.username
    PASSWORD = args.password
    ZIP_DIR  = args.zip_dir
    IDMC_URL = f"https://{POD}.informaticacloud.com"
    API_URL  = f"https://idmc-api.{POD}.informaticacloud.com"
    if args.no_zips:            DOWNLOAD_ZIPS = False
    if args.no_connections:     ENABLE_CONNECTIONS = False
    if args.no_catalog_sources: ENABLE_CATALOG_SOURCES = False
    if args.filter_connections:     FILTER_CONNECTIONS = args.filter_connections
    if args.filter_catalog_sources: FILTER_CATALOG_SOURCES = args.filter_catalog_sources

    print("=" * 60)
    print("CDGC Export Script")
    print("=" * 60)
    print(f"\nPod      : {POD}")
    print(f"IDMC URL : {IDMC_URL}")
    print(f"API URL  : {API_URL}\n")

    # Use a temp directory as the staging area — it is cleaned up after zipping.
    # If cdgc_export_temp_dir is set, use it (with {timestamp} substituted);
    # otherwise fall back to the OS default temp directory.
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    if TEMP_DIR_TEMPLATE:
        tmp_dir = TEMP_DIR_TEMPLATE.replace("{timestamp}", timestamp)
        os.makedirs(tmp_dir, exist_ok=True)
    else:
        tmp_dir = tempfile.mkdtemp(prefix="cdgc_export_")
    OUTPUT_DIR  = tmp_dir
    CS_DIR      = os.path.join(OUTPUT_DIR, "catalog_sources")
    CS_ZIPS_DIR = os.path.join(OUTPUT_DIR, "catalog_source_zips")
    CONN_DIR    = os.path.join(OUTPUT_DIR, "connections")

    try:
        print("[Auth] Logging in...")
        auth = login(IDMC_URL, USERNAME, PASSWORD)

        cs_count  = 0
        zip_count = 0
        connections = {}
        enc_csv     = None

        if ENABLE_CATALOG_SOURCES:
            cs_count  = export_catalog_sources(auth)
            zip_count = download_catalog_source_zips(auth)

        roles    = export_roles(auth)
        groups   = export_groups(auth)
        runtimes = export_runtime_environments(auth)

        if ENABLE_CONNECTIONS:
            connections = export_connections(auth)

        classifications = export_classifications(auth)

        print("\n  Scanning for encrypted fields...")
        enc_csv = export_encrypted_fields_csv(
            CONN_DIR if ENABLE_CONNECTIONS     else None,
            CS_DIR   if ENABLE_CATALOG_SOURCES else None,
            OUTPUT_DIR,
        )

        # -------------------------------------------------------------------------
        # Bundle the temp staging dir into a timestamped zip, then clean up.
        # The encrypted-fields CSV is stored with an _empty suffix so users know
        # it needs to be filled in before use on import.
        # -------------------------------------------------------------------------
        org_token = normalize_name_for_filename(auth.get("org_name", ""))
        zip_name  = f"export_{org_token}_{timestamp}.zip"
        os.makedirs(ZIP_DIR, exist_ok=True)
        zip_path  = os.path.join(ZIP_DIR, zip_name)

        print(f"\n[Bundling] Creating {zip_path} ...")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _dirs, files in os.walk(OUTPUT_DIR):
                for fname in files:
                    full_path = os.path.join(root, fname)
                    arc_name  = os.path.relpath(full_path, OUTPUT_DIR)
                    if re.match(r"encrypted_fields_\d{8}_\d{6}\.csv", arc_name):
                        zf.write(full_path, "encrypted_fields_empty.csv")
                    else:
                        zf.write(full_path, arc_name)
        print(f"  Bundled export → {zip_path}")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    print("\n" + "=" * 60)
    print("Export complete!")
    if ENABLE_CATALOG_SOURCES:
        print(f"  Catalog sources      : {cs_count}")
        if DOWNLOAD_ZIPS:
            print(f"  Custom source zips   : {zip_count}")
    print(f"  Roles                : {len(roles)}")
    print(f"  Groups               : {len(groups)}")
    print(f"  Runtime environments : {len(runtimes)}")
    if ENABLE_CONNECTIONS:
        print(f"  Connections          : {len(connections)}")
    print(f"  Classifications      : {len(classifications)}")
    if enc_csv:
        print(f"\n  Encrypted fields     : encrypted_fields_empty.csv (in zip)")
        print(f"  *** Fill in Value column, rename to encrypted_fields.csv, place in zip or next to script ***")
    print(f"\n  Export bundle        : {zip_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
