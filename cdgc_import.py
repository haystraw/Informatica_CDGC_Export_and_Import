"""
cdgc_import.py

Imports connections and CDGC catalog sources from the export/ folder into a
target IDMC org.

Rules (connections):
  - Skips any connection whose name already exists in the target org
  - Remaps runtimeEnvironmentId by matching the source runtime environment name
    in the target org. Falls back to first active runtime if no name match.
  - Strips read-only / org-specific system fields before POSTing
  - Replaces encrypted fields (********) with a placeholder, then auto-applies
    values from encrypted_fields.csv if present

Rules (catalog sources):
  - Skips any catalog source whose name already exists in the target org
  - Remaps ConnectionId (and Staging Connection) values by matching connection
    names between source and target orgs
  - Remaps Runtime Environment ID values the same way

Usage:
  python cdgc_import.py                           # import connections + catalog sources
  python cdgc_import.py --connections             # connections only
  python cdgc_import.py --catalog-sources         # catalog sources only
  python cdgc_import.py --encrypted-fields <csv>  # apply encrypted fields CSV only

Configure via environment variables or edit the constants below:
  IDMC_POD, IDMC_USERNAME, IDMC_PASSWORD, EXPORT_DIR
"""

import copy
import io
import os
import re
import sys
import json
import csv
import datetime
import argparse
import zipfile
import time
import requests


class _Tee:
    """Writes every print() to both the original stdout and an open log file."""
    def __init__(self, log_path):
        self._file   = open(log_path, "w", encoding="utf-8", buffering=1)
        self._stdout = sys.stdout
    def write(self, data):
        self._stdout.write(data)
        self._file.write(data)
    def flush(self):
        self._stdout.flush()
        self._file.flush()
    def close(self):
        self._file.close()
        sys.stdout = self._stdout
    def __getattr__(self, name):
        return getattr(self._stdout, name)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
POD        = os.getenv("IDMC_POD",      "dmp-us")
USERNAME   = os.getenv("IDMC_USERNAME", "your_username_here")
PASSWORD   = os.getenv("IDMC_PASSWORD", "your_password_here")
EXPORT_DIR   = os.getenv("EXPORT_DIR",    "./export")
CONN_DIR     = os.path.join(EXPORT_DIR, "connections")
CS_DIR       = os.path.join(EXPORT_DIR, "catalog_sources")
CS_ZIPS_DIR  = os.path.join(EXPORT_DIR, "catalog_source_zips")

# Connection fields to strip — system-assigned or org-specific
CONN_STRIP_FIELDS = {
    "id", "orgId", "createTime", "updateTime",
    "createdBy", "updatedBy", "majorUpdateTime", "federatedId",
}

MASKED_VALUE         = "********"
PLACEHOLDER          = "1234"
ENCRYPTED_FIELDS_CSV     = os.path.join(EXPORT_DIR, "encrypted_fields_connections.csv")
ENCRYPTED_RESOURCES_CSV  = os.path.join(EXPORT_DIR, "encrypted_fields_resources.csv")

# When True (default): encrypted blobs (org-specific AAA… values) are treated as equivalent
# during comparison — a source won't be updated just because the encrypted blob differs.
# Set False to always PUT sources that contain encrypted fields, even if nothing else changed.
IGNORE_ENCRYPTED_CHANGES = True

# ---------------------------------------------------------------------------
# Name filters (for troubleshooting — leave None to include everything)
# Set to a regex string to import only matching connections / catalog sources.
# Examples:
#   FILTER_CONNECTIONS     = r"Snowflake|Oracle"
#   FILTER_CATALOG_SOURCES = r"Lineage"
# ---------------------------------------------------------------------------
FILTER_CONNECTIONS     = None
FILTER_CATALOG_SOURCES = None

# ---------------------------------------------------------------------------
# Filter helper
# ---------------------------------------------------------------------------

def _name_matches(name, pattern):
    """Returns True if pattern is None/empty, or if name matches the regex."""
    if not pattern:
        return True
    return bool(re.search(pattern, name, re.IGNORECASE))


# HTTP status codes that are safe to retry (transient server-side errors)
_RETRY_STATUS_CODES = {408, 429, 502, 503, 504}
# Exceptions that are safe to retry
_RETRY_EXCEPTIONS   = (requests.exceptions.Timeout, requests.exceptions.ConnectionError)


def _call_with_retry(method, url, label="", max_retries=4, base_delay=3, **kwargs):
    """
    Calls method(url, **kwargs) and retries on transient failures:
      - HTTP 408 / 429 / 502 / 503 / 504
      - requests.exceptions.Timeout or ConnectionError

    Waits base_delay * attempt seconds between retries (3 s, 6 s, 9 s, …).
    Returns the last response (for HTTP errors) or raises on the final attempt.
    """
    prefix = f"             [{label}] " if label else "             "
    last_resp = None
    for attempt in range(max_retries):
        try:
            resp = method(url, **kwargs)
            if resp.status_code not in _RETRY_STATUS_CODES:
                return resp
            last_resp = resp
        except _RETRY_EXCEPTIONS as e:
            last_resp = None
            if attempt == max_retries - 1:
                raise
            wait = base_delay * (attempt + 1)
            print(f"{prefix}retrying in {wait}s after {type(e).__name__}...")
            time.sleep(wait)
            continue

        if attempt == max_retries - 1:
            return last_resp
        wait = base_delay * (attempt + 1)
        status = last_resp.status_code if last_resp else "?"
        print(f"{prefix}retrying in {wait}s after HTTP {status}...")
        time.sleep(wait)

    return last_resp  # should not reach here


def _get(url, headers, params=None, timeout=30):
    """GET with automatic retry on transient errors."""
    return _call_with_retry(requests.get, url, headers=headers, params=params, timeout=timeout)


# configOptions keys whose values are connection IDs
_CONN_ID_KEYS = {"ConnectionId", "Staging Connection"}

# configOptions keys whose values are runtime environment IDs
_RT_ID_KEYS = {"Runtime Environment"}

# Source runtime names that map to Informatica's cloud-hosted serverless agent in the target org
_SERVERLESS_RUNTIME_NAMES = {"MultiTenantServerless"}
_CLOUD_HOSTED_AGENT_NAME  = "Informatica Cloud Hosted Agent"


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def login(pod_url, username, password):
    # Step 1: v3 login
    login_resp = requests.post(
        f"{pod_url}/saas/public/core/v3/login",
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
    iics_url = raw_base_url[:-len("/saas")] if raw_base_url.endswith("/saas") else raw_base_url or pod_url

    if not session_id:
        raise RuntimeError(f"Login failed: no sessionId. Response: {login_data}")
    if not org_id:
        raise RuntimeError(f"Login failed: no orgId. Response: {login_data}")

    print(f"  Logged in as : {user_info.get('name', username)}")
    print(f"  Org          : {org_name} ({org_id})")
    print(f"  IICS URL     : {iics_url}")

    # Step 2: JWT token (required for catalog source API)
    token_resp = requests.post(
        f"{pod_url}/identity-service/api/v1/jwt/Token",
        params={"client_id": "cdlg_app", "nonce": "g3t69BWB49BHHNn", "access_code": ""},
        headers={
            "Accept":          "application/json",
            "INFA-SESSION-ID": session_id,
            "IDS-SESSION-ID":  session_id,
            "icSessionId":     session_id,
        },
        timeout=30,
    )
    token_resp.raise_for_status()
    access_token = token_resp.json().get("jwt_token")
    if not access_token:
        raise RuntimeError(f"JWT token step failed: {token_resp.json()}")

    api_url      = pod_url.replace("https://", "https://idmc-api.")
    cdgc_api_url = pod_url.replace("https://", "https://cdgc-api.")
    print(f"  API URL      : {api_url}")
    print(f"  CDGC API URL : {cdgc_api_url}")

    return {
        "session_id":   session_id,
        "access_token": access_token,
        "org_id":       org_id,
        "org_name":     org_name,
        "iics_url":     iics_url,
        "api_url":      api_url,
        "cdgc_api_url": cdgc_api_url,
        "pod_url":      pod_url,
    }


def session_headers(auth):
    """Session-based headers for /saas/api/v2/ calls."""
    return {
        "content-type":    "application/json",
        "Accept":          "application/json",
        "INFA-SESSION-ID": auth["session_id"],
        "IDS-SESSION-ID":  auth["session_id"],
        "icSessionId":     auth["session_id"],
    }


def cdgc_headers(auth):
    """JWT Bearer headers for /data360/ catalog source calls."""
    return {
        "X-INFA-ORG-ID": auth["org_id"],
        "IDS-SESSION-ID": auth["session_id"],
        "Authorization":  f"Bearer {auth['access_token']}",
        "Content-Type":   "application/json",
    }


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def get_runtime_environments(auth):
    """Returns the full list of runtime environment objects for the target org."""
    resp = _get(
        f"{auth['iics_url']}/saas/api/v2/runtimeEnvironment",
        headers=session_headers(auth),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json() if isinstance(resp.json(), list) else []


def is_runtime_active(rt):
    agents = rt.get("agents", [])
    return any(a.get("active") is True for a in agents)


def get_existing_connections(auth):
    """Returns a dict of { connection_name: id } for the target org."""
    resp = _get(
        f"{auth['iics_url']}/saas/api/v2/connection",
        headers=session_headers(auth),
        timeout=30,
    )
    resp.raise_for_status()
    return {c["name"]: c["id"] for c in resp.json() if "name" in c}


def load_source_lookups():
    """
    Loads the source org lookup files exported by cdgc_export.py.
    Returns (source_rt_id_to_name, source_conn_id_to_name, source_cs_name_to_id).
    """
    rt_lookup_path = os.path.join(EXPORT_DIR, "runtime_environments_lookup.json")
    source_rt_id_to_name = {}
    if os.path.isfile(rt_lookup_path):
        with open(rt_lookup_path, encoding="utf-8") as f:
            rt_name_to_id = json.load(f)
        source_rt_id_to_name = {v: k for k, v in rt_name_to_id.items()}
        print(f"  Loaded source runtime env lookup  : {len(source_rt_id_to_name)} entries")
    else:
        print(f"  WARNING: runtime_environments_lookup.json not found — runtime env name matching disabled")

    conn_lookup_path = os.path.join(EXPORT_DIR, "connections_lookup.json")
    source_conn_id_to_name = {}
    if os.path.isfile(conn_lookup_path):
        with open(conn_lookup_path, encoding="utf-8") as f:
            conn_name_to_id = json.load(f)
        source_conn_id_to_name = {v: k for k, v in conn_name_to_id.items()}
        print(f"  Loaded source connection lookup   : {len(source_conn_id_to_name)} entries")
    else:
        print(f"  WARNING: connections_lookup.json not found — connection ID remapping disabled")

    cs_id_lookup_path = os.path.join(EXPORT_DIR, "catalog_sources_id_lookup.json")
    source_cs_name_to_id = {}
    if os.path.isfile(cs_id_lookup_path):
        with open(cs_id_lookup_path, encoding="utf-8") as f:
            source_cs_name_to_id = json.load(f)
        print(f"  Loaded source catalog source IDs  : {len(source_cs_name_to_id)} entries")
    else:
        print(f"  WARNING: catalog_sources_id_lookup.json not found — links.csv ID remapping disabled")

    return source_rt_id_to_name, source_conn_id_to_name, source_cs_name_to_id


# ---------------------------------------------------------------------------
# Connection import
# ---------------------------------------------------------------------------

def strip_conn_fields(conn):
    return {k: v for k, v in conn.items() if k not in CONN_STRIP_FIELDS}


def find_encrypted_fields(obj, prefix=""):
    """Recursively find fields with the Informatica masked sentinel '********'."""
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


def replace_masked_values(obj):
    """Replace '********' with PLACEHOLDER so the POST is accepted by the API."""
    if isinstance(obj, dict):
        return {k: replace_masked_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_masked_values(v) for v in obj]
    elif obj == MASKED_VALUE:
        return PLACEHOLDER
    return obj


# Informatica encrypted config values are base64 blobs whose first 3 chars are always
# 'AAA' (= 0x0000 null header in their proprietary encryption format).  The 4th char
# varies by version/length byte (e.g. 'A', 'B', '6', '+', 'O', 'a', …).
# We also require: pure base64 alphabet, at least 80 chars long, optional trailing '='.
# This is very unlikely to match any real config value (URLs, usernames, paths, etc.)
_INFA_ENCRYPTED_RE = re.compile(r'^AAA[A-Za-z0-9+/]{77,}={0,2}$')

# Keys checked (in order) to build a meaningful array-element identifier in the path
_PATH_ID_KEYS = ("optionGroupName", "capabilityName", "globalConfigurationName", "key", "name")

# Keys used to sort arrays of dicts during comparison so element order doesn't matter.
# The first matching key found in the array's elements is used as the sort key.
_COMPARE_SORT_KEYS = ("capabilityName", "globalConfigurationName", "optionGroupName", "key", "name")

# globalConfigOptions sections that are system-managed and excluded from comparison.
# "Schedules" is org-specific (already stripped during remap); "Associations" is
# auto-populated by Informatica when resources are linked and is never portable.
_CS_STRIP_GLOBAL_CONFIG_NAMES = {"Schedules", "Associations"}


def _is_encrypted_cs_value(v):
    """True if the value looks like an Informatica org-encrypted string.
    These long AAA-prefixed base64 blobs are org-specific and will not work in a
    different org — they must be re-entered manually after import."""
    return isinstance(v, str) and bool(_INFA_ENCRYPTED_RE.match(v))


def clear_encrypted_cs_fields(payload):
    """
    Recursively walks the entire catalog source payload.  For every Informatica-
    encrypted value (long AAA-prefixed base64 blob) found at any depth, clears it
    to "" (so the POST is accepted) and records its exact location as a JSONPath-
    style string, e.g.:

      typeOptions.configurationProperties[optionGroupName="IICS OptionGroup"]
        .configOptions[key="IICS Password"].values[0]

    Array elements that are dicts are identified by the first matching key from
    _PATH_ID_KEYS; plain-value array elements fall back to a numeric index.

    Returns a list of path strings, one per cleared value.
    """
    cleared = []

    def _walk(obj, path):
        if isinstance(obj, dict):
            for k, v in obj.items():
                child = f"{path}.{k}" if path else k
                obj[k] = _walk(v, child)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, dict):
                    id_key = next((k for k in _PATH_ID_KEYS if k in item), None)
                    elem_id = f'[{id_key}="{item[id_key]}"]' if id_key else f"[{i}]"
                else:
                    elem_id = f"[{i}]"
                obj[i] = _walk(item, f"{path}{elem_id}")
        elif _is_encrypted_cs_value(obj):
            cleared.append(path)
            # Leave the value intact — clearing it causes a mandatory-field 500.
            # The resource will be created but scans will fail until the user
            # re-enters the correct value in the UI (see encrypted fields CSV).
        return obj

    _walk(payload, "")
    return cleared


def _apply_cs_path(obj, path_str, value):
    """
    Navigate a catalog source payload using a JSONPath-style string produced by
    clear_encrypted_cs_fields / find_encrypted_cs_fields, then set the terminal
    element to *value*.

    Segment types recognised:
      key          — plain dict key  (e.g. 'typeOptions')
      [name="val"] — list element whose dict field 'name' equals 'val'
      [n]          — numeric list index
    """
    segments = []
    pos = 0
    while pos < len(path_str):
        if path_str[pos] == '.':
            pos += 1
        elif path_str[pos] == '[':
            end = path_str.index(']', pos)
            token = path_str[pos + 1:end]
            if '=' in token:
                k, v = token.split('=', 1)
                segments.append(('match', k.strip(), v.strip().strip('"')))
            else:
                segments.append(('index', int(token.strip())))
            pos = end + 1
        else:
            end = pos
            while end < len(path_str) and path_str[end] not in '.[]':
                end += 1
            segments.append(('key', path_str[pos:end]))
            pos = end

    current = obj
    for seg in segments[:-1]:
        if seg[0] == 'key':
            current = current[seg[1]]
        elif seg[0] == 'match':
            current = next(item for item in current if item.get(seg[1]) == seg[2])
        else:
            current = current[seg[1]]

    last = segments[-1]
    if last[0] == 'key':
        current[last[1]] = value
    elif last[0] == 'index':
        current[last[1]] = value
    # 'match' as the final segment is not expected for scalar values


def load_encrypted_resources_csv(csv_path):
    """
    Loads a filled-in encrypted_fields_resources.csv into:
      { catalog_source_name: { json_path: value, ... }, ... }
    Rows with an empty Value column are skipped.
    """
    result = {}
    if not os.path.isfile(csv_path):
        return result
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name  = (row.get("Catalog Source Name") or "").strip()
            path  = (row.get("JSON Path") or "").strip()
            value = (row.get("Value") or "").strip()
            if name and path and value:
                result.setdefault(name, {})[path] = value
    return result


def load_encrypted_fields_csv(csv_path):
    """
    Loads a filled-in encrypted_fields.csv into:
      { connection_name: { field_path: value, ... }, ... }
    Rows with an empty Value are skipped.
    """
    updates = {}
    if not os.path.isfile(csv_path):
        return updates
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            conn_name  = row.get("Connection Name", "").strip()
            field_path = row.get("Parameter Name", "").strip()
            value      = row.get("Value", "").strip()
            if conn_name and field_path and value:
                updates.setdefault(conn_name, {})[field_path] = value
    return updates


def apply_encrypted_fields_for_connection(auth, conn_name, conn_id, enc_data):
    """Applies encrypted field values for a single connection via PARTIAL update."""
    if not enc_data:
        return

    payload = {"@type": "connection"}
    for field_path, value in enc_data.items():
        parts = field_path.split(".", 1)
        if len(parts) == 2:
            parent, child = parts
            if parent not in payload:
                payload[parent] = {}
            payload[parent][child] = value
        else:
            payload[field_path] = value

    resp = requests.post(
        f"{auth['iics_url']}/saas/api/v2/connection/{conn_id}",
        headers={**session_headers(auth), "Update-Mode": "PARTIAL"},
        json=payload,
        timeout=30,
    )
    field_list = ", ".join(enc_data.keys())
    if resp.ok:
        print(f"             Encrypted fields applied: {field_list}")
    else:
        print(f"             Encrypted fields FAILED  : {field_list}")
        print(f"             {resp.status_code}: {resp.text[:200]}")


def _resolve_conn_runtime(source_rt_name, source_rt_id, target_runtimes):
    """
    Resolves the target runtime environment for a connection import.
    Returns (target_rt_id, target_rt_name, target_agent_id, match_reason).

    Special case: source runtimes in _SERVERLESS_RUNTIME_NAMES (e.g. MultiTenantServerless)
    are mapped to _CLOUD_HOSTED_AGENT_NAME in the target org before attempting name match.
    """
    def first_agent_id(rt):
        agents = rt.get("agents", [])
        return agents[0]["id"] if agents else None

    # Serverless → cloud-hosted agent mapping
    # Check both name and id — source org may store 'MultiTenantServerless' as the raw ID
    if source_rt_name in _SERVERLESS_RUNTIME_NAMES or source_rt_id in _SERVERLESS_RUNTIME_NAMES:
        for rt in target_runtimes:
            if rt.get("name") == _CLOUD_HOSTED_AGENT_NAME:
                return (
                    rt["id"], rt["name"], first_agent_id(rt),
                    f"serverless→'{_CLOUD_HOSTED_AGENT_NAME}' (source='{source_rt_name or source_rt_id}')",
                )
        # Cloud hosted agent not in target — fall through to normal resolution

    for rt in target_runtimes:
        if rt.get("name") == source_rt_name:
            return rt["id"], rt["name"], first_agent_id(rt), "name match"
    for rt in target_runtimes:
        if is_runtime_active(rt):
            return rt["id"], rt["name"], first_agent_id(rt), "first active (no name match)"
    if target_runtimes:
        rt = target_runtimes[0]
        return rt["id"], rt["name"], first_agent_id(rt), "first available (no active agents found)"
    raise RuntimeError("No runtime environments found in target org.")


def import_connections(auth, target_runtimes, source_rt_id_to_name, update_mode=False, run_dir="."):
    if not os.path.isdir(CONN_DIR):
        print(f"  Connections folder not found: {CONN_DIR} — skipping.")
        return

    conn_files = [f for f in os.listdir(CONN_DIR) if f.endswith(".json")]
    if not conn_files:
        print("  No connection files found to import.")
        return

    print(f"  Found {len(conn_files)} connection file(s) in {CONN_DIR}/")

    print("\n  Fetching existing connections in target org...")
    existing = get_existing_connections(auth)
    print(f"  Target org has {len(existing)} existing connection(s).")

    enc_csv_data = load_encrypted_fields_csv(ENCRYPTED_FIELDS_CSV)
    if enc_csv_data:
        print(f"\n  Loaded encrypted fields CSV: {ENCRYPTED_FIELDS_CSV}")
        print(f"  {len(enc_csv_data)} connection(s) have values staged")
    else:
        print(f"\n  No encrypted_fields_connections.csv found at {ENCRYPTED_FIELDS_CSV} — encrypted fields will need manual entry")

    if update_mode:
        print("\n  UPDATE MODE: existing connections will be updated with export payload")

    conn_run_dir = os.path.join(run_dir, "connections")
    os.makedirs(conn_run_dir, exist_ok=True)

    print()
    created, updated, skipped, failed = 0, 0, 0, 0
    enc_log = []

    for fname in sorted(conn_files):
        fpath = os.path.join(CONN_DIR, fname)
        with open(fpath, encoding="utf-8") as f:
            conn = json.load(f)

        name           = conn.get("name", fname)
        if not _name_matches(name, FILTER_CONNECTIONS):
            continue
        source_rt_id   = conn.get("runtimeEnvironmentId", "")
        source_rt_name = source_rt_id_to_name.get(source_rt_id, "")

        try:
            target_rt_id, target_rt_name, target_agent_id, match_reason = _resolve_conn_runtime(
                source_rt_name, source_rt_id, target_runtimes)
        except RuntimeError as e:
            print(f"  ERROR    : {name}  — {e}")
            failed += 1
            continue

        enc     = find_encrypted_fields(conn)
        payload = strip_conn_fields(conn)
        payload = replace_masked_values(payload)
        payload["runtimeEnvironmentId"] = target_rt_id
        if target_agent_id:
            payload["agentId"] = target_agent_id
        else:
            payload.pop("agentId", None)
        if "connParams" in payload:
            if "agentId" in payload["connParams"] and target_agent_id:
                payload["connParams"]["agentId"] = target_agent_id
            if "agentGroupId" in payload["connParams"]:
                payload["connParams"]["agentGroupId"] = target_rt_id
            if "orgId" in payload["connParams"]:
                payload["connParams"]["orgId"] = auth["org_id"]

        if name in existing:
            if not update_mode:
                print(f"  SKIP     : {name}  (already exists)")
                skipped += 1
                continue
            conn_id = existing[name]
            print(f"  UPDATING : {name}  ({conn_id})")
            print(f"             Runtime env -> '{target_rt_name}' ({match_reason})")
            resp = requests.post(
                f"{auth['iics_url']}/saas/api/v2/connection/{conn_id}",
                headers={**session_headers(auth), "Update-Mode": "PARTIAL"},
                json=payload,
                timeout=30,
            )
            if resp.ok:
                print(f"             Updated OK")
                updated += 1
                conn_safe = re.sub(r'[\\/*?:"<>|]', "_", name)
                with open(os.path.join(conn_run_dir, conn_safe + ".json"), "w", encoding="utf-8") as _cf:
                    json.dump(payload, _cf, indent=4)
                apply_encrypted_fields_for_connection(auth, name, conn_id, enc_csv_data.get(name, {}))
                for fp in enc:
                    if fp not in enc_csv_data.get(name, {}):
                        enc_log.append((name, fp))
            else:
                print(f"             FAILED {resp.status_code}: {resp.text[:300]}")
                failed += 1
        else:
            print(f"  CREATING : {name}")
            print(f"             Runtime env -> '{target_rt_name}' ({match_reason})")
            resp = requests.post(
                f"{auth['iics_url']}/saas/api/v2/connection",
                headers=session_headers(auth),
                json=payload,
                timeout=30,
            )
            if resp.ok:
                result  = resp.json()
                conn_id = result.get("id")
                print(f"             Created with id: {conn_id}")
                created += 1
                conn_safe = re.sub(r'[\\/*?:"<>|]', "_", name)
                with open(os.path.join(conn_run_dir, conn_safe + ".json"), "w", encoding="utf-8") as _cf:
                    json.dump(payload, _cf, indent=4)
                apply_encrypted_fields_for_connection(auth, name, conn_id, enc_csv_data.get(name, {}))
                for fp in enc:
                    if fp not in enc_csv_data.get(name, {}):
                        enc_log.append((name, fp))
                if enc and not enc_csv_data.get(name):
                    print(f"             Encrypted fields need values: {', '.join(enc)}")
            else:
                print(f"             FAILED {resp.status_code}: {resp.text[:300]}")
                failed += 1

    report_path = os.path.join(run_dir, "import_encrypted_fields_connections.csv")
    try:
        with open(report_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Connection Name", "Parameter Name", "Value"])
            for conn_name, field_path in enc_log:
                existing_val = enc_csv_data.get(conn_name, {}).get(field_path, "")
                writer.writerow([conn_name, field_path, existing_val])
        if enc_log:
            uniq = len({r[0] for r in enc_log})
            print(f"\n  Encrypted fields report: {report_path}")
            print(f"  ({len(enc_log)} field(s) across {uniq} connection(s) still need values)")
        else:
            print(f"\n  Encrypted fields report: {report_path}  (none found)")
    except PermissionError:
        print(f"\n  WARNING: Could not write encrypted fields report — permission denied.")

    print(f"\n  Connections — Created: {created}  Updated: {updated}  Skipped: {skipped}  Failed: {failed}")


# ---------------------------------------------------------------------------
# Catalog source import
# ---------------------------------------------------------------------------

def get_existing_catalog_sources(auth):
    """Returns a dict of { catalog_source_name: id } for the target org."""
    endpoint  = f"{auth['cdgc_api_url']}/ccgf-catalog-source-management/api/v1/datasources"
    offset, limit, results = 0, 50, {}
    while True:
        resp = _get(
            endpoint,
            headers=cdgc_headers(auth),
            params={"offset": offset, "limit": limit},
            timeout=30,
        )
        resp.raise_for_status()
        data  = resp.json()
        batch = data if isinstance(data, list) else (data.get("datasources") or data.get("catalogSources") or data.get("items") or [])
        for cs in batch:
            if cs.get("name") and cs.get("id"):
                results[cs["name"]] = cs["id"]
        if len(batch) < limit:
            break
        offset += limit
    return results


_CS_STRIP_FIELDS = {
    "id", "endOfLife", "seedVersion", "isDeleted", "createdBy",
    "lastModifiedBy", "createdTime", "lastModifiedTime", "modelVersion",
    "additionalMetadata",  # system-populated; export always has null, target has a dict
}


def fetch_catalog_source(auth, cs_id):
    """Fetch a single catalog source's full payload by ID from the target org."""
    url  = f"{auth['api_url']}/data360/catalog-source-management/v1/catalogsources/{cs_id}"
    resp = _get(url, headers=cdgc_headers(auth), timeout=30)
    return resp.json() if resp.ok else None


def _normalize_for_compare(obj):
    """
    Recursively strip system fields and replace encrypted blobs with a sentinel
    so that two payloads can be compared purely on their functional content.

    Arrays of dicts are sorted by a stable identifying key (_COMPARE_SORT_KEYS)
    so that element order differences don't trigger false UPDATE detections.

    Encrypted values (org-specific AAA… blobs) become "__ENCRYPTED__"; the
    deep-equal check in payloads_equal treats that as a wildcard so that a
    plaintext export value at the same path as an encrypted target value does
    not trigger an unnecessary UPDATE.

    File Details configOptions have their values stripped: the staging file-path
    UUID (values[1]) is assigned fresh on each upload and will always differ
    between orgs, so it must not trigger an UPDATE.
    """
    if isinstance(obj, dict):
        # "File Details" configOption: values = [filename, staging_uuid].
        # The staging UUID is org-specific — blank the values so both sides compare equal.
        if obj.get("key") == "File Details" and "values" in obj:
            return {
                k: ([] if k == "values" else _normalize_for_compare(v))
                for k, v in obj.items()
                if k not in _CS_STRIP_FIELDS
            }
        return {
            k: _normalize_for_compare(v)
            for k, v in obj.items()
            if k not in _CS_STRIP_FIELDS
        }
    if isinstance(obj, list):
        # Drop system-managed globalConfigOptions sections before recursing so
        # both the export and live-target sides are compared on the same basis.
        if obj and isinstance(obj[0], dict) and "globalConfigurationName" in obj[0]:
            obj = [
                item for item in obj
                if item.get("globalConfigurationName") not in _CS_STRIP_GLOBAL_CONFIG_NAMES
            ]
        normalized = [_normalize_for_compare(v) for v in obj]
        # Sort arrays of dicts by the first matching identifying key so that
        # capabilities / globalConfigOptions order doesn't cause false diffs.
        if normalized and isinstance(normalized[0], dict):
            for sort_key in _COMPARE_SORT_KEYS:
                if any(sort_key in item for item in normalized):
                    try:
                        normalized.sort(
                            key=lambda x: json.dumps(x.get(sort_key, ""), sort_keys=True)
                        )
                    except Exception:
                        pass
                    break
        return normalized
    if isinstance(obj, str) and _INFA_ENCRYPTED_RE.match(obj):
        return "__ENCRYPTED__" if IGNORE_ENCRYPTED_CHANGES else obj
    return obj


def payloads_equal(remapped, current):
    """
    Returns True if the remapped export payload and the live target payload are
    functionally identical (ignoring system fields, array ordering, and encrypted
    blobs).  When IGNORE_ENCRYPTED_CHANGES is True, __ENCRYPTED__ matches any
    string value so a plaintext export value that is stored encrypted in the
    target org does not trigger an unnecessary UPDATE.
    """
    n1 = _normalize_for_compare(copy.deepcopy(remapped))
    n2 = _normalize_for_compare(copy.deepcopy(current))

    def _deep_equal(a, b):
        # __ENCRYPTED__ is a wildcard — if either side is the sentinel and both
        # sides are strings, treat them as equal (same credential, different encoding).
        if IGNORE_ENCRYPTED_CHANGES and (a == "__ENCRYPTED__" or b == "__ENCRYPTED__"):
            return isinstance(a, str) and isinstance(b, str)
        if type(a) != type(b):
            return False
        if isinstance(a, dict):
            if set(a.keys()) != set(b.keys()):
                return False
            return all(_deep_equal(a[k], b[k]) for k in a)
        if isinstance(a, list):
            if len(a) != len(b):
                return False
            return all(_deep_equal(x, y) for x, y in zip(a, b))
        return a == b

    return _deep_equal(n1, n2)


def _diff_payloads(remapped, current, max_diffs=10):
    """
    Returns a list of human-readable strings describing differences between
    two normalized payloads.  Used to explain why a catalog source is flagged
    as UPDATE rather than SKIP.

    Improvements over a plain recursive diff:
    - Array elements that are dicts are identified by their _PATH_ID_KEYS value
      (e.g. capabilityName, key) rather than a numeric index, so paths are
      readable even after sorting.
    - When list lengths differ, reports which named items are only in the export
      or only in the target instead of just "list len X → Y".
    - Scalar diffs show both values clearly without truncating key information.
    """
    n1 = _normalize_for_compare(copy.deepcopy(remapped))
    n2 = _normalize_for_compare(copy.deepcopy(current))
    diffs = []

    def _id_of(item):
        """Return the first identifying value from a dict, or None."""
        if not isinstance(item, dict):
            return None
        for k in _PATH_ID_KEYS:
            if k in item:
                return str(item[k])
        return None

    def _fmt(v, max_len=120):
        s = json.dumps(v)
        return s if len(s) <= max_len else s[:max_len - 3] + "..."

    def _elem_path(path, item, index):
        """Build a path segment using the item's identifier key if available."""
        id_val = _id_of(item)
        return f'{path}["{id_val}"]' if id_val else f"{path}[{index}]"

    def _walk(a, b, path):
        if len(diffs) >= max_diffs:
            return
        # __ENCRYPTED__ wildcard — same credential, different org encoding; skip
        if IGNORE_ENCRYPTED_CHANGES and (a == "__ENCRYPTED__" or b == "__ENCRYPTED__"):
            if isinstance(a, str) and isinstance(b, str):
                return
        if type(a) != type(b):
            diffs.append(f"{path}: type changed ({type(a).__name__} → {type(b).__name__})")
            return
        if isinstance(a, dict):
            for k in sorted(set(a) | set(b)):
                if len(diffs) >= max_diffs:
                    return
                child = f"{path}.{k}" if path else k
                if k not in a:
                    diffs.append(f"{child}: [not in export, present in target]")
                elif k not in b:
                    diffs.append(f"{child}: [in export, not in target]")
                else:
                    _walk(a[k], b[k], child)
        elif isinstance(a, list):
            if len(a) != len(b):
                # For lists of named dicts, report which items are unique to each side
                a_ids = [_id_of(x) for x in a]
                b_ids = [_id_of(x) for x in b]
                if any(v is not None for v in a_ids + b_ids):
                    a_id_set = {v for v in a_ids if v is not None}
                    b_id_set = {v for v in b_ids if v is not None}
                    only_export = sorted(a_id_set - b_id_set)
                    only_target = sorted(b_id_set - a_id_set)
                    if only_export:
                        diffs.append(f"{path}: in export only — {', '.join(repr(x) for x in only_export)}")
                    if only_target:
                        diffs.append(f"{path}: in target only — {', '.join(repr(x) for x in only_target)}")
                    if not only_export and not only_target:
                        diffs.append(f"{path}: {len(a)} item(s) → {len(b)} item(s)")
                else:
                    diffs.append(f"{path}: {len(a)} item(s) → {len(b)} item(s)")
            else:
                for i, (x, y) in enumerate(zip(a, b)):
                    _walk(x, y, _elem_path(path, x, i))
        else:
            if a != b:
                diffs.append(f"{path}:\n                       export: {_fmt(a)}\n                       target: {_fmt(b)}")

    _walk(n1, n2, "")
    if len(diffs) >= max_diffs:
        diffs.append("(… more differences not shown)")
    return diffs


def _resolve_cs_runtime(source_rt_name, source_rt_id, target_runtimes):
    """
    Resolves the target runtime environment for a catalog source config option.
    Returns (target_rt_id, match_reason).

    Special case: source runtimes in _SERVERLESS_RUNTIME_NAMES (e.g. MultiTenantServerless)
    are mapped to _CLOUD_HOSTED_AGENT_NAME in the target org before attempting name match.
    """
    target_rt_name_to_id = {rt["name"]: rt["id"] for rt in target_runtimes}

    # Serverless → cloud-hosted agent mapping
    # Check both name and id — source org may store 'MultiTenantServerless' as the raw ID
    if source_rt_name in _SERVERLESS_RUNTIME_NAMES or source_rt_id in _SERVERLESS_RUNTIME_NAMES:
        if _CLOUD_HOSTED_AGENT_NAME in target_rt_name_to_id:
            return (
                target_rt_name_to_id[_CLOUD_HOSTED_AGENT_NAME],
                f"serverless→'{_CLOUD_HOSTED_AGENT_NAME}' (source='{source_rt_name or source_rt_id}')",
            )
        # Cloud hosted agent not in target — fall through to normal resolution

    if source_rt_name and source_rt_name in target_rt_name_to_id:
        return target_rt_name_to_id[source_rt_name], "name match"

    for rt in target_runtimes:
        if is_runtime_active(rt):
            return rt["id"], f"first active (source='{source_rt_name or source_rt_id}' not found)"

    if target_runtimes:
        return target_runtimes[0]["id"], "first available (no active agents found)"

    raise RuntimeError("No runtime environments found in target org.")


def remap_catalog_source(cs, source_rt_id_to_name, source_conn_id_to_name,
                          target_runtimes, target_conn_name_to_id):
    """
    Deep-copies the catalog source payload and remaps all org-specific IDs:
      - configOptions with key in _CONN_ID_KEYS  -> target connection ID (by name)
      - configOptions with key in _RT_ID_KEYS    -> target runtime env ID (by name)

    Returns (remapped_payload, warnings).
    """
    warnings = []

    def remap_config_options(config_options):
        for opt in config_options:
            key    = opt.get("key", "")
            values = opt.get("values") or []
            if not values:
                continue

            if key in _CONN_ID_KEYS:
                source_id   = values[0]
                source_name = source_conn_id_to_name.get(source_id, "")
                if source_name and source_name in target_conn_name_to_id:
                    opt["values"] = [target_conn_name_to_id[source_name]]
                elif source_name:
                    warnings.append(
                        f"Connection '{source_name}' (key={key}) not found in target org — ID not remapped"
                    )
                else:
                    warnings.append(
                        f"Unknown source connection ID '{source_id}' (key={key}) — not remapped"
                    )

            elif key in _RT_ID_KEYS:
                source_id   = values[0]
                # If the stored value is already a portable symbolic name (e.g.
                # "MultiTenantServerless"), leave it unchanged — it is not an
                # org-specific UUID and will resolve correctly in the target org.
                if source_id in _SERVERLESS_RUNTIME_NAMES:
                    continue
                source_name = source_rt_id_to_name.get(source_id, "")
                try:
                    target_rt_id, reason = _resolve_cs_runtime(source_name, source_id, target_runtimes)
                    opt["values"] = [target_rt_id]
                    if reason != "name match":
                        warnings.append(f"Runtime env (key={key}): {reason}")
                except RuntimeError as e:
                    warnings.append(f"Runtime env (key={key}): {e}")

    def walk(obj):
        if isinstance(obj, dict):
            if "configOptions" in obj:
                remap_config_options(obj["configOptions"])
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    cs_copy = copy.deepcopy(cs)

    # Strip system-managed globalConfigOptions sections — not portable across orgs
    if "globalConfigOptions" in cs_copy:
        cs_copy["globalConfigOptions"] = [
            g for g in cs_copy["globalConfigOptions"]
            if g.get("globalConfigurationName") not in _CS_STRIP_GLOBAL_CONFIG_NAMES
        ]

    walk(cs_copy)
    return cs_copy, warnings


def preflight_type_check(cs_files):
    """
    Scans source catalog source files and reports unique types.
    Returns a dict: { type_name: {"count": int, "custom": bool, "names": [str]} }
    """
    summary = {}
    for fname in cs_files:
        fpath = os.path.join(CS_DIR, fname)
        with open(fpath, encoding="utf-8") as f:
            cs = json.load(f)
        t         = cs.get("type", "Unknown")
        is_custom = cs.get("custom", False)
        if t not in summary:
            summary[t] = {"count": 0, "custom": is_custom, "names": []}
        summary[t]["count"] += 1
        summary[t]["names"].append(cs.get("name", fname))
    return summary


def get_datasource_types(auth):
    """Returns the set of datasource type names registered in the target org."""
    resp = requests.get(
        f"{auth['cdgc_api_url']}/ccgf-catalog-source-management/api/v1/datasourceTypes",
        headers=cdgc_headers(auth),
        params={"offset": 0, "limit": 10000},
        timeout=30,
    )
    if not resp.ok:
        print(f"  WARNING: Could not fetch datasource types ({resp.status_code}) — type check skipped.")
        return set()
    return {t["name"] for t in resp.json().get("datasourceTypes", [])}


def get_missing_datasource_types(auth, needed_type_names):
    """Returns sorted list of type names needed but not present in the target org."""
    existing = get_datasource_types(auth)
    return sorted(t for t in needed_type_names if t not in existing)


def create_datasource_types(auth, missing):
    """Creates the given datasource type names in the target org (no prompt)."""
    types_url = f"{auth['cdgc_api_url']}/ccgf-catalog-source-management/api/v1/datasourceTypes"
    for type_name in missing:
        resp = requests.post(
            types_url,
            headers=cdgc_headers(auth),
            json={"id": "", "name": type_name, "description": "", "category": "Custom"},
            timeout=30,
        )
        if resp.ok:
            print(f"  CREATED  type : {type_name}")
        elif resp.status_code == 409:
            print(f"  EXISTS   type : {type_name}  (already present)")
        else:
            print(f"  FAILED   type : {type_name}  {resp.status_code}: {resp.text[:150]}")


def _cs_label(fn, cs, custom_no_id_fnames):
    """Returns a display label suffix for a catalog source based on its type."""
    if not cs.get("custom"):
        return ""
    if fn in custom_no_id_fnames:
        return " [custom/internal]"
    return " [custom/lineage]"


def build_catalog_source_plan(auth, target_runtimes, source_rt_id_to_name,
                               source_conn_id_to_name, source_cs_name_to_id,
                               target_conn_name_to_id, enc_res_map):
    """
    Builds the full execution plan for catalog source import.  For each source:
      - Remaps IDs (runtime env, connections)
      - Detects and logs encrypted fields
      - Applies values from encrypted_fields_resources.csv where available
      - For existing sources: fetches current payload and compares to determine
        UPDATE (changed) vs SKIP (unchanged)

    Returns (plan, missing_types) where plan is a list of dicts with keys:
      fname, cs, name, label, action, payload, warnings, cleared, applied_paths, enc_res_log
    and missing_types is a sorted list of custom type names not in the target org.
    """
    if not os.path.isdir(CS_DIR):
        return [], []

    cs_files = [f for f in os.listdir(CS_DIR) if f.endswith(".json")]
    if not cs_files:
        return [], []

    existing = get_existing_catalog_sources(auth)

    # Sort into three groups: non-custom → custom/internal → custom/lineage
    seen_names = {}
    all_cs = []
    for fname in sorted(cs_files):
        with open(os.path.join(CS_DIR, fname), encoding="utf-8") as f:
            cs = json.load(f)
        name = cs.get("name", fname)
        if not _name_matches(name, FILTER_CATALOG_SOURCES):
            continue
        if name in seen_names:
            print(f"  WARNING: Duplicate catalog source name '{name}' in both "
                  f"'{seen_names[name]}' and '{fname}' — skipping '{fname}'")
            continue
        seen_names[name] = fname
        all_cs.append((fname, cs))

    if FILTER_CATALOG_SOURCES and len(all_cs) < len(cs_files):
        print(f"  Filter '{FILTER_CATALOG_SOURCES}' applied: {len(all_cs)} of {len(cs_files)} source(s) match")

    non_custom      = [(fn, cs) for fn, cs in all_cs if not cs.get("custom")]
    custom_no_ids   = []
    custom_with_ids = []
    for fn, cs in all_cs:
        if not cs.get("custom"):
            continue
        safe_name = re.sub(r'[\\/*?:"<>|]', "_", cs.get("name", fn))
        local_zip = os.path.join(CS_ZIPS_DIR, safe_name + ".zip")
        if zip_has_id_links(local_zip):
            custom_with_ids.append((fn, cs))
        else:
            custom_no_ids.append((fn, cs))

    ordered = non_custom + custom_no_ids + custom_with_ids
    custom_no_id_fnames = {fn for fn, _ in custom_no_ids}

    # Collect custom types needed
    type_summary = preflight_type_check(cs_files)
    custom_type_names = {tn for tn, info in type_summary.items() if info["custom"]}
    missing_types = get_missing_datasource_types(auth, custom_type_names)

    plan = []
    for fname, cs in ordered:
        name  = cs.get("name", fname)
        label = _cs_label(fname, cs, custom_no_id_fnames)

        payload, warnings = remap_catalog_source(
            cs, source_rt_id_to_name, source_conn_id_to_name,
            target_runtimes, target_conn_name_to_id,
        )

        cleared = clear_encrypted_cs_fields(payload)

        # Apply pre-filled values from encrypted_fields_resources.csv
        cs_enc_map    = enc_res_map.get(name, {})
        applied_paths = set()
        for json_path in cleared:
            if json_path in cs_enc_map:
                try:
                    _apply_cs_path(payload, json_path, cs_enc_map[json_path])
                    applied_paths.add(json_path)
                except Exception:
                    pass

        enc_res_log = [(name, jp) for jp in cleared if jp not in applied_paths]

        if name not in existing:
            action = "CREATE"
            diff_reasons = []
        else:
            current = fetch_catalog_source(auth, existing[name])
            if current and payloads_equal(payload, current):
                action = "SKIP"
                diff_reasons = []
            else:
                action = "UPDATE"
                diff_reasons = _diff_payloads(payload, current) if current else ["(could not fetch current payload)"]

        plan.append({
            "fname": fname, "cs": cs, "name": name, "label": label,
            "action": action, "payload": payload, "warnings": warnings,
            "cleared": cleared, "applied_paths": applied_paths,
            "enc_res_log": enc_res_log,
            "existing_id": existing.get(name),
            "diff_reasons": diff_reasons,
        })

    return plan, missing_types


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


def _set_file_details(cs, filename, new_file_path_id):
    """Updates File Details values in-place on the payload dict."""
    for group in cs.get("typeOptions", {}).get("configurationProperties", []):
        if group.get("optionGroupName") == "Custom OptionGroup":
            for opt in group.get("configOptions", []):
                if opt.get("key") == "File Details":
                    opt["values"] = [filename, new_file_path_id]
                    return True
    return False


def upload_staging_file(auth, local_path, filename):
    """
    Uploads a zip file to the target org's staging area.

    POST /ccgf-metadata-staging/api/v1/staging/files
         ?serviceFunction=catalog-source-staging-producer

    Returns the new filePath UUID on success, or None on failure.
    """
    staging_url = (
        f"{auth['cdgc_api_url']}/ccgf-metadata-staging/api/v1/staging/files"
        f"?serviceFunction=catalog-source-staging-producer"
    )
    with open(local_path, "rb") as f:
        resp = requests.post(
            staging_url,
            headers={
                "X-INFA-ORG-ID":  auth["org_id"],
                "IDS-SESSION-ID": auth["session_id"],
                "Authorization":  f"Bearer {auth['access_token']}",
                # Do NOT set Content-Type — requests sets it with the correct boundary
            },
            files={"file": (filename, f, "application/x-zip-compressed")},
            timeout=120,
        )
    if resp.ok:
        return resp.json().get("filePath")
    print(f"             Staging upload FAILED {resp.status_code}: {resp.text[:200]}")
    return None


def upload_staging_file_bytes(auth, zip_bytes, filename):
    """
    Same as upload_staging_file but accepts in-memory bytes instead of a file path.
    Used to upload a patched zip without writing it to disk.
    """
    staging_url = (
        f"{auth['cdgc_api_url']}/ccgf-metadata-staging/api/v1/staging/files"
        f"?serviceFunction=catalog-source-staging-producer"
    )
    resp = requests.post(
        staging_url,
        headers={
            "X-INFA-ORG-ID":  auth["org_id"],
            "IDS-SESSION-ID": auth["session_id"],
            "Authorization":  f"Bearer {auth['access_token']}",
        },
        files={"file": (filename, io.BytesIO(zip_bytes), "application/x-zip-compressed")},
        timeout=120,
    )
    if resp.ok:
        return resp.json().get("filePath")
    print(f"             Staging upload FAILED {resp.status_code}: {resp.text[:200]}")
    return None


_UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}://',
                      re.IGNORECASE | re.MULTILINE)


def zip_has_id_links(local_zip):
    """
    Returns True if links.csv inside the zip contains UUID-prefixed paths
    (e.g. '61d29b37-a14a-3661-ac76-cfaed8d1deb4://...'), indicating cross-resource
    lineage that needs ID remapping.  Returns False for internal-only links.
    Returns False if the zip doesn't exist or has no links.csv.
    """
    if not os.path.isfile(local_zip):
        return False
    try:
        with zipfile.ZipFile(local_zip, "r") as zf:
            if "links.csv" not in zf.namelist():
                return False
            content = zf.read("links.csv").decode("utf-8", errors="replace")
            return bool(_UUID_RE.search(content))
    except Exception:
        return False


def patch_zip_links(local_zip, source_id_to_target_id):
    """
    Opens local_zip, replaces all source catalog source UUIDs with their target
    equivalents in every file (most importantly links.csv), and returns the
    patched zip as an in-memory bytes object.

    The links.csv format uses UUIDs as path prefixes, e.g.:
      {uuid}://schema/table~com.infa.odin...

    Returns (patched_bytes, replacements_made) where patched_bytes is None if
    no replacements were needed (original zip should be used as-is).
    """
    buf = io.BytesIO()
    replacements_made = 0

    with zipfile.ZipFile(local_zip, "r") as zin, \
         zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            # Patch all text files, not just links.csv — some zips include
            # resource CSVs that may also reference catalog source IDs.
            try:
                content = data.decode("utf-8")
                for src_id, tgt_id in source_id_to_target_id.items():
                    if src_id in content:
                        content = content.replace(src_id, tgt_id)
                        replacements_made += 1
                data = content.encode("utf-8")
            except UnicodeDecodeError:
                pass  # binary file — copy unchanged
            zout.writestr(item, data)

    if replacements_made:
        return buf.getvalue(), replacements_made
    return None, 0


def import_catalog_sources(auth, target_runtimes, source_rt_id_to_name, source_conn_id_to_name,
                           source_cs_name_to_id=None, run_dir=".", plan=None):
    if source_cs_name_to_id is None:
        source_cs_name_to_id = {}

    payloads_dir     = os.path.join(run_dir, "catalog_sources")
    patched_zips_dir = os.path.join(run_dir, "catalog_source_zips")
    os.makedirs(payloads_dir,     exist_ok=True)
    os.makedirs(patched_zips_dir, exist_ok=True)
    enc_res_log = []  # [(cs_name, json_path), ...]

    if plan is None:
        # Standalone call (no pre-built plan) — build plan now
        enc_res_map = load_encrypted_resources_csv(ENCRYPTED_RESOURCES_CSV)
        target_conn_name_to_id = get_existing_connections(auth)
        plan, _ = build_catalog_source_plan(
            auth, target_runtimes, source_rt_id_to_name, source_conn_id_to_name,
            source_cs_name_to_id, target_conn_name_to_id, enc_res_map,
        )

    create_endpoint = f"{auth['cdgc_api_url']}/ccgf-catalog-source-management/api/v1/datasources"
    update_base_url = f"{auth['api_url']}/data360/catalog-source-management/v1/catalogsources"

    # Maps source org catalog source ID -> target org catalog source ID.
    source_id_to_target_id = {}

    print()
    created, updated, skipped, failed = 0, 0, 0, 0

    for item in plan:
        fname         = item["fname"]
        cs            = item["cs"]
        name          = item["name"]
        label         = item["label"]
        action        = item["action"]
        payload       = item["payload"]
        warnings      = item["warnings"]
        cleared       = item["cleared"]
        applied_paths = item["applied_paths"]

        if action == "SKIP":
            print(f"  SKIP     : {name}{label}  (no changes)")
            skipped += 1
            src_id = source_cs_name_to_id.get(name)
            if src_id and item.get("existing_id"):
                source_id_to_target_id[src_id] = item["existing_id"]
            continue

        is_update    = action == "UPDATE"
        action_label = "UPDATING" if is_update else "CREATING"
        print(f"  {action_label} : {name}{'  ' + label if cs.get('custom') else ''}")
        for w in warnings:
            print(f"  WARNING  : {w}")
        for json_path in cleared:
            if json_path in applied_paths:
                print(f"             Applied encrypted value from CSV: {json_path}")
            else:
                print(f"  WARNING  : Encrypted value (org-specific) — re-enter in UI after import: {json_path}")
                enc_res_log.append((name, json_path))

        # For custom sources: patch the zip's links.csv with remapped IDs,
        # then upload the patched zip to the target org's staging.
        if cs.get("custom"):
            orig_filename, _ = _get_file_details(payload)
            if orig_filename:
                safe_name = re.sub(r'[\\/*?:"<>|]', "_", name)
                local_zip = os.path.join(CS_ZIPS_DIR, safe_name + ".zip")

                if not os.path.isfile(local_zip):
                    # No exported zip — create a placeholder so the source can be created.
                    os.makedirs(CS_ZIPS_DIR, exist_ok=True)
                    with zipfile.ZipFile(local_zip, "w") as zf:
                        zf.writestr("placeholder.txt",
                                    f"Placeholder for '{name}'.\n"
                                    "Re-upload the real zip file via the CDGC UI and re-run the scan.")
                    print(f"             No exported zip found — created placeholder (re-upload real file via UI)")
                    zip_to_upload = local_zip
                    zip_bytes     = None
                else:
                    # Patch the zip: replace source org IDs with target org IDs in links.csv
                    if source_id_to_target_id:
                        zip_bytes, n_replacements = patch_zip_links(local_zip, source_id_to_target_id)
                        if zip_bytes:
                            print(f"             Patched links.csv: {n_replacements} ID replacement(s)")
                        else:
                            print(f"             links.csv: no matching IDs to replace")
                    else:
                        zip_bytes, n_replacements = None, 0
                        print(f"             links.csv: no ID map available yet (no non-custom sources created)")
                    zip_to_upload = local_zip  # fallback path for upload_staging_file

                print(f"             Uploading zip: {os.path.basename(local_zip)}")
                if zip_bytes is not None:
                    # Save patched zip to run_dir for reference
                    patched_zip_path = os.path.join(patched_zips_dir, safe_name + ".zip")
                    with open(patched_zip_path, "wb") as _pf:
                        _pf.write(zip_bytes)
                    # Upload the in-memory patched zip directly
                    new_file_path = upload_staging_file_bytes(auth, zip_bytes, orig_filename)
                else:
                    new_file_path = upload_staging_file(auth, zip_to_upload, orig_filename)

                if new_file_path:
                    _set_file_details(payload, orig_filename, new_file_path)
                    print(f"             File Details updated  (filePath={new_file_path})")
                else:
                    print(f"             Zip upload failed — proceeding without file (will likely fail)")

        # Save the final altered payload for reference
        safe_name_for_payload = re.sub(r'[\\/*?:"<>|]', "_", name)
        payload_path = os.path.join(payloads_dir, safe_name_for_payload + ".json")
        with open(payload_path, "w", encoding="utf-8") as _pf:
            json.dump(payload, _pf, indent=4)

        try:
            if is_update:
                target_id = item["existing_id"]
                resp = _call_with_retry(
                    requests.put, f"{update_base_url}/{target_id}",
                    label="PUT retry", headers=cdgc_headers(auth), json=payload, timeout=120,
                )
            else:
                # For POST, a 408 may mean the server created the resource but timed out
                # responding.  After each failed attempt, check by name before retrying
                # to avoid creating a duplicate.
                resp = None
                for _attempt in range(4):
                    try:
                        resp = requests.post(
                            create_endpoint,
                            headers=cdgc_headers(auth), json=payload, timeout=120,
                        )
                    except _RETRY_EXCEPTIONS as _e:
                        resp = None
                        print(f"             retrying after {type(_e).__name__}...")
                        time.sleep(3 * (_attempt + 1))
                        continue

                    if resp.ok or resp.status_code not in _RETRY_STATUS_CODES:
                        break

                    # Transient HTTP error — check if the resource was actually created
                    # before retrying, to avoid duplicates.
                    print(f"             HTTP {resp.status_code} — checking if source was created...")
                    refreshed = get_existing_catalog_sources(auth)
                    if name in refreshed:
                        print(f"             Source exists despite timeout — using existing id")
                        resp = type("_FakeResp", (), {
                            "ok": True, "json": lambda s: {"id": refreshed[name]}
                        })()
                        break
                    wait = 3 * (_attempt + 1)
                    print(f"             Not found — retrying in {wait}s...")
                    time.sleep(wait)

        except _RETRY_EXCEPTIONS as _e:
            print(f"             FAILED — {type(_e).__name__}: {_e}")
            failed += 1
            continue

        if resp is None:
            print(f"             FAILED — no response after retries")
            failed += 1
            continue

        if resp.ok:
            resp_data = resp.json()
            if is_update:
                job_id = resp_data.get("jobId", "?")
                print(f"             Updated  (jobId={job_id})")
                updated += 1
                target_id = item["existing_id"]
            else:
                target_id = resp_data.get("id", "?")
                print(f"             Created with id: {target_id}")
                created += 1
            # Record source→target ID mapping for patching later custom zips
            src_id = source_cs_name_to_id.get(name)
            if src_id and target_id and target_id != "?":
                source_id_to_target_id[src_id] = target_id
        else:
            print(f"             FAILED {resp.status_code}: {resp.text[:300]}")
            failed += 1

    report_path = os.path.join(run_dir, "import_encrypted_fields_resources.csv")
    try:
        with open(report_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Catalog Source Name", "JSON Path", "Value"])
            for cs_name, json_path in enc_res_log:
                writer.writerow([cs_name, json_path, ""])
        if enc_res_log:
            uniq = len({r[0] for r in enc_res_log})
            print(f"\n  Encrypted resource fields: {report_path}")
            print(f"  ({len(enc_res_log)} field(s) across {uniq} catalog source(s) — fill in Value column and re-apply)")
        else:
            print(f"\n  Encrypted resource fields report: {report_path}  (none found)")
    except PermissionError:
        print(f"\n  WARNING: Could not write resource encrypted fields report — permission denied.")

    print(f"\n  Catalog Sources — Created: {created}  Updated: {updated}  Skipped: {skipped}  Failed: {failed}")


# ---------------------------------------------------------------------------
# Standalone: apply encrypted fields CSV to existing connections
# ---------------------------------------------------------------------------

def apply_encrypted_fields(auth, csv_file):
    """
    Reads a filled-in encrypted_fields CSV and applies each value to its
    connection via a PARTIAL update POST to /saas/api/v2/connection/{id}.
    """
    print(f"\n{'=' * 60}")
    print(f"Applying encrypted fields from: {csv_file}")
    print(f"{'=' * 60}")

    resp = requests.get(
        f"{auth['iics_url']}/saas/api/v2/connection",
        headers=session_headers(auth),
        timeout=30,
    )
    resp.raise_for_status()
    conn_list   = resp.json() if isinstance(resp.json(), list) else []
    conn_lookup = {c["name"]: c["id"] for c in conn_list if "name" in c and "id" in c}
    print(f"  Found {len(conn_lookup)} connection(s) in target org\n")

    updates, skipped_empty = {}, 0
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            conn_name  = row.get("Connection Name", "").strip()
            field_path = row.get("Parameter Name", "").strip()
            value      = row.get("Value", "").strip()
            if not conn_name or not field_path:
                continue
            if not value:
                skipped_empty += 1
                continue
            updates.setdefault(conn_name, {})[field_path] = value

    if not updates:
        print("  No values to apply.")
        if skipped_empty:
            print(f"  ({skipped_empty} row(s) skipped — Value column empty)")
        return

    print(f"  {len(updates)} connection(s) to update, {skipped_empty} row(s) skipped\n")
    applied, failed = 0, 0

    for conn_name, fields in updates.items():
        conn_id = conn_lookup.get(conn_name)
        if not conn_id:
            print(f"  SKIP  : '{conn_name}' — not found in target org")
            failed += 1
            continue

        payload = {"@type": "connection"}
        for field_path, value in fields.items():
            parts = field_path.split(".", 1)
            if len(parts) == 2:
                parent, child = parts
                if parent not in payload:
                    payload[parent] = {}
                payload[parent][child] = value
            else:
                payload[field_path] = value

        resp = requests.post(
            f"{auth['iics_url']}/saas/api/v2/connection/{conn_id}",
            headers={
                "Content-Type": "application/json",
                "Accept":       "application/json",
                "icSessionId":  auth["session_id"],
                "Update-Mode":  "PARTIAL",
            },
            json=payload,
            timeout=30,
        )
        field_list = ", ".join(fields.keys())
        if resp.ok:
            print(f"  UPDATED : {conn_name}  ({field_list})")
            applied += 1
        else:
            print(f"  FAILED  : {conn_name}  ({field_list})")
            print(f"            {resp.status_code}: {resp.text[:200]}")
            failed += 1

    print(f"\n  Applied: {applied}  Failed: {failed}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CDGC Import — connections and catalog sources")
    parser.add_argument("--connections",     action="store_true",
                        help="Import connections only (default: both)")
    parser.add_argument("--catalog-sources", action="store_true",
                        help="Import catalog sources only (default: both)")
    parser.add_argument("--update",          action="store_true",
                        help="Update existing connections with export payload (default: skip existing)")
    parser.add_argument("--encrypted-fields", metavar="CSV_FILE",
                        help="Apply encrypted fields from a specific CSV file only — skips all other steps")
    args = parser.parse_args()

    # If neither specific flag is set, run both
    run_connections     = args.connections or (not args.connections and not args.catalog_sources)
    run_catalog_sources = args.catalog_sources or (not args.connections and not args.catalog_sources)

    pod_url = f"https://{POD}.informaticacloud.com"

    # Create a timestamped run folder and start logging to it
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir   = os.path.join("import_runs", f"import_{timestamp}")
    os.makedirs(run_dir, exist_ok=True)
    sys.stdout = _Tee(os.path.join(run_dir, "import.log"))

    print("=" * 60)
    print("CDGC Import")
    print("=" * 60)
    print(f"\nPOD      : {POD}")
    print(f"Pod URL  : {pod_url}")
    print(f"Source   : {EXPORT_DIR}/")
    print(f"Run dir  : {run_dir}/")

    print("\n[Auth] Logging in...")
    auth = login(pod_url, USERNAME, PASSWORD)

    # Mode: apply encrypted fields only
    if args.encrypted_fields:
        if not os.path.isfile(args.encrypted_fields):
            print(f"\nERROR: File not found: {args.encrypted_fields}")
            sys.exit(1)
        apply_encrypted_fields(auth, args.encrypted_fields)
        return

    # Load shared source org lookups (runtime envs + connections)
    print("\n[Lookups] Loading source org lookup files...")
    source_rt_id_to_name, source_conn_id_to_name, source_cs_name_to_id = load_source_lookups()

    # Fetch target runtime environments once — shared by both import steps
    print("\n  Fetching runtime environments in target org...")
    target_runtimes = get_runtime_environments(auth)
    print(f"  Target org has {len(target_runtimes)} runtime environment(s).")
    for rt in target_runtimes:
        agents = rt.get("agents", [])
        active = any(a.get("active") is True for a in agents)
        print(f"    - {rt.get('name')} ({rt.get('id')})  agents={len(agents)}  active={active}")

    # ---------------------------------------------------------------------------
    # Pre-flight: build full plan and display before executing anything
    # ---------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("[Pre-flight]")
    print("=" * 60)

    existing_conns = get_existing_connections(auth)

    # --- Connections plan ---
    conn_plan = []
    if run_connections and os.path.isdir(CONN_DIR):
        conn_files = sorted(f for f in os.listdir(CONN_DIR) if f.endswith(".json"))
        for fname in conn_files:
            with open(os.path.join(CONN_DIR, fname), encoding="utf-8") as f:
                conn = json.load(f)
            cname  = conn.get("name", fname)
            if not _name_matches(cname, FILTER_CONNECTIONS):
                continue
            action = ("UPDATE" if args.update else "SKIP") if cname in existing_conns else "CREATE"
            conn_plan.append({"name": cname, "action": action})

    # --- Catalog sources plan (includes comparisons for existing sources) ---
    cs_plan      = []
    missing_types = []
    if run_catalog_sources:
        enc_res_map = load_encrypted_resources_csv(ENCRYPTED_RESOURCES_CSV)
        if enc_res_map:
            print(f"\n  Encrypted resource fields CSV loaded: {ENCRYPTED_RESOURCES_CSV}")
        print("\n  Building catalog source plan (comparing existing sources to export)...")
        cs_plan, missing_types = build_catalog_source_plan(
            auth, target_runtimes, source_rt_id_to_name, source_conn_id_to_name,
            source_cs_name_to_id, existing_conns, enc_res_map,
        )

    # --- Display consolidated plan ---
    if conn_plan:
        print(f"\n  Connections ({len(conn_plan)} total):")
        for item in conn_plan:
            print(f"    {item['action']:<6} : {item['name']}")

    if missing_types:
        print(f"\n  Custom datasource type(s) missing from target org — will be created:")
        for t in missing_types:
            print(f"    CREATE type : {t}")

    if cs_plan:
        counts = {"CREATE": 0, "UPDATE": 0, "SKIP": 0}
        for item in cs_plan:
            counts[item["action"]] += 1
        print(f"\n  Catalog Sources ({len(cs_plan)} total — "
              f"Create: {counts['CREATE']}  Update: {counts['UPDATE']}  Skip: {counts['SKIP']}):")
        for item in cs_plan:
            enc_note = f"  [{len(item['enc_res_log'])} encrypted field(s) need re-entry]" if item["enc_res_log"] else ""
            print(f"    {item['action']:<6} : {item['name']}{item['label']}{enc_note}")
            for reason in item.get("diff_reasons", []):
                print(f"             ~ {reason}")

    # --- Single confirmation prompt ---
    print()
    confirm = input("  Proceed with import? (YES to continue, Enter to abort): ").strip()
    if confirm.upper() != "YES":
        print("  Aborted.")
        return

    # ---------------------------------------------------------------------------
    # Execute
    # ---------------------------------------------------------------------------
    if run_connections:
        print("\n" + "=" * 60)
        print("[Connections]")
        print("=" * 60)
        import_connections(auth, target_runtimes, source_rt_id_to_name, update_mode=args.update,
                          run_dir=run_dir)

    if run_catalog_sources:
        print("\n" + "=" * 60)
        print("[Catalog Sources]")
        print("=" * 60)
        if missing_types:
            print("\n  Creating missing datasource types...")
            create_datasource_types(auth, missing_types)
        import_catalog_sources(auth, target_runtimes, source_rt_id_to_name, source_conn_id_to_name,
                               source_cs_name_to_id, run_dir=run_dir, plan=cs_plan)

    print("\n" + "=" * 60)
    print("Import complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
