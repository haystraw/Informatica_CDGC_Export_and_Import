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
import os
import sys
import json
import csv
import datetime
import argparse
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
POD        = os.getenv("IDMC_POD",      "dmp-us")
USERNAME   = os.getenv("IDMC_USERNAME", "your_username_here")
PASSWORD   = os.getenv("IDMC_PASSWORD", "your_password_here")
EXPORT_DIR = os.getenv("EXPORT_DIR",    "./export")
CONN_DIR   = os.path.join(EXPORT_DIR, "connections")
CS_DIR     = os.path.join(EXPORT_DIR, "catalog_sources")

# Connection fields to strip — system-assigned or org-specific
CONN_STRIP_FIELDS = {
    "id", "orgId", "createTime", "updateTime",
    "createdBy", "updatedBy", "majorUpdateTime", "federatedId",
}

MASKED_VALUE         = "********"
PLACEHOLDER          = "1234"
ENCRYPTED_FIELDS_CSV = os.path.join(EXPORT_DIR, "encrypted_fields.csv")

# Catalog source import behaviour:
#   False (default) — skip catalog sources that already exist in the target org
#   True            — attempt to overwrite/update existing catalog sources (not yet implemented;
#                     will attempt creation and the API will return a conflict error)
CS_UPDATE_EXISTING = False

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
    resp = requests.get(
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
    resp = requests.get(
        f"{auth['iics_url']}/saas/api/v2/connection",
        headers=session_headers(auth),
        timeout=30,
    )
    resp.raise_for_status()
    return {c["name"]: c["id"] for c in resp.json() if "name" in c}


def load_source_lookups():
    """
    Loads the source org lookup files exported by cdgc_export.py.
    Returns (source_rt_id_to_name, source_conn_id_to_name).
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

    return source_rt_id_to_name, source_conn_id_to_name


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


def import_connections(auth, target_runtimes, source_rt_id_to_name, update_mode=False):
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
        print(f"\n  No encrypted_fields.csv found at {ENCRYPTED_FIELDS_CSV} — encrypted fields will need manual entry")

    if update_mode:
        print("\n  UPDATE MODE: existing connections will be updated with export payload")

    print()
    created, updated, skipped, failed = 0, 0, 0, 0
    enc_log = []

    for fname in sorted(conn_files):
        fpath = os.path.join(CONN_DIR, fname)
        with open(fpath, encoding="utf-8") as f:
            conn = json.load(f)

        name           = conn.get("name", fname)
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
                apply_encrypted_fields_for_connection(auth, name, conn_id, enc_csv_data.get(name, {}))
                for fp in enc:
                    if fp not in enc_csv_data.get(name, {}):
                        enc_log.append((name, fp))
                if enc and not enc_csv_data.get(name):
                    print(f"             Encrypted fields need values: {', '.join(enc)}")
            else:
                print(f"             FAILED {resp.status_code}: {resp.text[:300]}")
                failed += 1

    if enc_log:
        timestamp   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(EXPORT_DIR, f"import_encrypted_fields_report_{timestamp}.csv")
        try:
            with open(report_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Connection Name", "Parameter Name", "Value"])
                for conn_name, field_path in enc_log:
                    existing_val = enc_csv_data.get(conn_name, {}).get(field_path, "")
                    writer.writerow([conn_name, field_path, existing_val])
            uniq = len({r[0] for r in enc_log})
            print(f"\n  Encrypted fields report: {report_path}")
            print(f"  ({len(enc_log)} field(s) across {uniq} connection(s) still need values)")
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
        resp = requests.get(
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

    # Strip schedule configuration — schedules are org-specific and not portable
    if "globalConfigOptions" in cs_copy:
        cs_copy["globalConfigOptions"] = [
            g for g in cs_copy["globalConfigOptions"]
            if g.get("globalConfigurationName") != "Schedules"
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


def ensure_datasource_types(auth, needed_type_names):
    """
    Checks that all needed custom datasource type names exist in the target org.
    Reports any that are missing, prompts the user, and creates them if confirmed.
    """
    existing = get_datasource_types(auth)
    if not existing and not needed_type_names:
        return

    missing = sorted(t for t in needed_type_names if t not in existing)
    if not missing:
        print(f"  All required datasource types are present in the target org.")
        return

    print(f"\n  The following datasource type(s) are missing from the target org:")
    for t in missing:
        print(f"    - {t}")
    print()
    confirm = input("  Type YES to auto-create these types before importing, or press Enter to skip: ").strip()
    if confirm != "YES":
        print("  Skipped — catalog sources using missing types will likely fail.")
        return

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
        else:
            print(f"  FAILED   type : {type_name}  {resp.status_code}: {resp.text[:150]}")


def import_catalog_sources(auth, target_runtimes, source_rt_id_to_name, source_conn_id_to_name):
    if not os.path.isdir(CS_DIR):
        print(f"  Catalog sources folder not found: {CS_DIR} — skipping.")
        return

    cs_files = [f for f in os.listdir(CS_DIR) if f.endswith(".json")]
    if not cs_files:
        print("  No catalog source files found to import.")
        return

    print(f"  Found {len(cs_files)} catalog source file(s) in {CS_DIR}/")

    print("\n  Fetching existing catalog sources in target org...")
    existing = get_existing_catalog_sources(auth)
    print(f"  Target org has {len(existing)} existing catalog source(s).")

    # Pre-flight: report catalog source types, create/skip plan, and custom type check
    print("\n  [Pre-flight] Catalog source types in export:")
    type_summary = preflight_type_check(cs_files)
    custom_type_names = set()
    for type_name, info in sorted(type_summary.items()):
        flag = "  ** CUSTOM TYPE **" if info["custom"] else ""
        print(f"    {type_name}: {info['count']} source(s){flag}")
        if info["custom"]:
            custom_type_names.add(type_name)

    print("\n  [Pre-flight] Import plan:")
    to_create, to_skip = [], []
    for fname in sorted(cs_files):
        fpath = os.path.join(CS_DIR, fname)
        with open(fpath, encoding="utf-8") as f:
            cs_name = json.load(f).get("name", fname)
        if cs_name in existing and not CS_UPDATE_EXISTING:
            to_skip.append(cs_name)
        else:
            to_create.append(cs_name)
    for name in to_create:
        print(f"    CREATE : {name}")
    for name in to_skip:
        print(f"    SKIP   : {name}  (already exists)")
    print(f"\n    Total: {len(to_create)} to create, {len(to_skip)} to skip.")

    if custom_type_names:
        ensure_datasource_types(auth, custom_type_names)

    print("\n  Fetching target org connections for ID remapping...")
    target_conn_name_to_id = get_existing_connections(auth)
    print(f"  Found {len(target_conn_name_to_id)} connection(s) in target org.")

    endpoint = f"{auth['cdgc_api_url']}/ccgf-catalog-source-management/api/v1/datasources"

    print()
    created, skipped, failed = 0, 0, 0

    for fname in sorted(cs_files):
        fpath = os.path.join(CS_DIR, fname)
        with open(fpath, encoding="utf-8") as f:
            cs = json.load(f)

        name = cs.get("name", fname)

        if name in existing:
            if not CS_UPDATE_EXISTING:
                print(f"  SKIP     : {name}  (already exists)")
                skipped += 1
                continue
            print(f"  EXISTS   : {name}  (CS_UPDATE_EXISTING=True — attempting overwrite)")

        payload, warnings = remap_catalog_source(
            cs, source_rt_id_to_name, source_conn_id_to_name,
            target_runtimes, target_conn_name_to_id,
        )

        print(f"  CREATING : {name}")
        for w in warnings:
            print(f"  WARNING  : {w}")

        resp = requests.post(endpoint, headers=cdgc_headers(auth), json=payload, timeout=30)
        if resp.ok:
            cs_id = resp.json().get("id", "?")
            print(f"             Created with id: {cs_id}")
            created += 1
        else:
            print(f"             FAILED {resp.status_code}: {resp.text[:300]}")
            failed += 1

    print(f"\n  Catalog Sources — Created: {created}  Skipped: {skipped}  Failed: {failed}")


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

    print("=" * 60)
    print("CDGC Import")
    print("=" * 60)
    print(f"\nPOD      : {POD}")
    print(f"Pod URL  : {pod_url}")
    print(f"Source   : {EXPORT_DIR}/")

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
    source_rt_id_to_name, source_conn_id_to_name = load_source_lookups()

    # Fetch target runtime environments once — shared by both import steps
    print("\n  Fetching runtime environments in target org...")
    target_runtimes = get_runtime_environments(auth)
    print(f"  Target org has {len(target_runtimes)} runtime environment(s).")
    for rt in target_runtimes:
        agents = rt.get("agents", [])
        active = any(a.get("active") is True for a in agents)
        print(f"    - {rt.get('name')} ({rt.get('id')})  agents={len(agents)}  active={active}")

    if run_connections:
        print("\n" + "=" * 60)
        print("[Connections]")
        print("=" * 60)
        import_connections(auth, target_runtimes, source_rt_id_to_name, update_mode=args.update)

    if run_catalog_sources:
        print("\n" + "=" * 60)
        print("[Catalog Sources]")
        print("=" * 60)
        import_catalog_sources(auth, target_runtimes, source_rt_id_to_name, source_conn_id_to_name)

    print("\n" + "=" * 60)
    print("Import complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
