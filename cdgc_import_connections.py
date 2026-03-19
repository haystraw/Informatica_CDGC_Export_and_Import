"""
cdgc_import_connections.py

Imports connections from the export/connections/ folder into a target IDMC org.

Rules:
  - Skips any connection whose name already exists in the target org
  - Remaps runtimeEnvironmentId by matching the source runtime environment name
    in the target org. If no name match is found, uses the first active runtime
    environment found in the target org.
  - Strips all read-only / org-specific system fields before POSTing
  - Replaces encrypted fields (********) with a placeholder on import, then
    auto-applies any encrypted_fields_*.csv found in EXPORT_DIR afterwards

Usage:
  # Full import (connections + auto-apply encrypted fields CSV if found)
  python cdgc_import_connections.py

  # Apply encrypted fields CSV only (connections already imported)
  python cdgc_import_connections.py --encrypted-fields ./export/encrypted_fields_20260312_120000.csv

Configure the constants below or set environment variables:
  IDMC_POD        e.g. dmp-us
  IDMC_USERNAME
  IDMC_PASSWORD
  EXPORT_DIR      path to the export folder (default: ./export)
"""

import os
import sys
import json
import re
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

# Fields to strip before POSTing — system-assigned or org-specific
# Only strip true system-assigned fields that are meaningless in the target org
# Everything else (including retryNetworkError, vaultEnabled, etc.) is kept as-is
STRIP_FIELDS = {
    "id",
    "orgId",
    "createTime",
    "updateTime",
    "createdBy",
    "updatedBy",
    "majorUpdateTime",
    "federatedId",
}

# ---------------------------------------------------------------------------
# Auth  (matches cdgc_export.py exactly)
# ---------------------------------------------------------------------------

def login(pod_url, username, password):
    # Step 1: v3 login
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
    iics_url = raw_base_url[:-len("/saas")] if raw_base_url.endswith("/saas") else raw_base_url or pod_url

    if not session_id:
        raise RuntimeError(f"Login failed: no sessionId. Response: {login_data}")
    if not org_id:
        raise RuntimeError(f"Login failed: no orgId. Response: {login_data}")

    print(f"  Logged in as : {user_info.get('name', username)}")
    print(f"  Org          : {org_name} ({org_id})")
    print(f"  IICS URL     : {iics_url}")

    # Step 2: JWT token
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

    return {
        "session_id":   session_id,
        "access_token": access_token,
        "org_id":       org_id,
        "org_name":     org_name,
        "iics_url":     iics_url,
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_existing_connections(auth):
    """Returns a dict of { connection_name: id } for the target org."""
    resp = requests.get(
        f"{auth['iics_url']}/saas/api/v2/connection",
        headers=session_headers(auth),
        timeout=30,
    )
    resp.raise_for_status()
    return {c["name"]: c["id"] for c in resp.json() if "name" in c}


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
    """
    A runtime environment is considered active if it has at least one
    agent with active=True. (There is no top-level status field.)
    """
    agents = rt.get("agents", [])
    return any(a.get("active") is True for a in agents)


def resolve_runtime_env(source_rt_name, source_rt_id, target_runtimes):
    """
    Finds the best matching runtime environment in the target org:
      1. Match by name (preferred)
      2. Fall back to first runtime environment that has an active agent
      3. Last resort: first runtime environment in the list

    Returns (id, name, agent_id, matched_by) or raises if none available.
    """
    def first_agent_id(rt):
        agents = rt.get("agents", [])
        return agents[0]["id"] if agents else None

    # Try name match first
    for rt in target_runtimes:
        if rt.get("name") == source_rt_name:
            return rt["id"], rt["name"], first_agent_id(rt), "name match"

    # Fall back to first runtime with an active agent
    for rt in target_runtimes:
        if is_runtime_active(rt):
            return rt["id"], rt["name"], first_agent_id(rt), "first active agent (no name match)"

    # Last resort: first in list
    if target_runtimes:
        rt = target_runtimes[0]
        return rt["id"], rt["name"], first_agent_id(rt), "first available (no active agents found)"

    raise RuntimeError("No runtime environments found in target org.")


def strip_fields(conn):
    return {k: v for k, v in conn.items() if k not in STRIP_FIELDS}


MASKED_VALUE = "********"
PLACEHOLDER  = "1234"


ENCRYPTED_FIELDS_CSV = os.path.join(EXPORT_DIR, "encrypted_fields.csv")


def load_encrypted_fields_csv(csv_path):
    """
    Loads a filled-in encrypted_fields.csv into a dict:
      { connection_name: { field_path: value, ... }, ... }
    Rows with empty Value are skipped.
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
    """
    Applies encrypted field values for a single connection via PARTIAL update.
    enc_data: { field_path: value }
    """
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

    url = f"{auth['iics_url']}/saas/api/v2/connection/{conn_id}"
    resp = requests.post(url, headers={**session_headers(auth), "Update-Mode": "PARTIAL"},
                         json=payload, timeout=30)
    field_list = ", ".join(enc_data.keys())
    if resp.ok:
        print(f"             Encrypted fields applied: {field_list}")
    else:
        print(f"             Encrypted fields FAILED  : {field_list}")
        print(f"             {resp.status_code}: {resp.text[:200]}")


# ---------------------------------------------------------------------------
# Main import logic
# ---------------------------------------------------------------------------

def import_connections(auth, update_mode=False):
    if not os.path.isdir(CONN_DIR):
        raise RuntimeError(f"Connections export folder not found: {CONN_DIR}")

    conn_files = [f for f in os.listdir(CONN_DIR) if f.endswith(".json")]
    if not conn_files:
        print("  No connection files found to import.")
        return

    print(f"\n  Found {len(conn_files)} connection file(s) in {CONN_DIR}/")

    print("\n  Fetching existing connections in target org...")
    existing = get_existing_connections(auth)
    print(f"  Target org has {len(existing)} existing connection(s).")

    print("\n  Fetching runtime environments in target org...")
    target_runtimes = get_runtime_environments(auth)
    print(f"  Target org has {len(target_runtimes)} runtime environment(s).")
    for rt in target_runtimes:
        agents = rt.get("agents", [])
        active = any(a.get("active") is True for a in agents)
        print(f"    - {rt.get('name')} ({rt.get('id')})  agents={len(agents)}  active={active}")

    # Load source runtime lookup (name -> id) and invert to (id -> name)
    rt_lookup_path = os.path.join(EXPORT_DIR, "runtime_environments_lookup.json")
    source_rt_id_to_name = {}
    if os.path.isfile(rt_lookup_path):
        with open(rt_lookup_path, encoding="utf-8") as f:
            rt_name_to_id = json.load(f)
        source_rt_id_to_name = {v: k for k, v in rt_name_to_id.items()}
        print(f"\n  Loaded source runtime lookup: {len(source_rt_id_to_name)} entries")
    else:
        print(f"\n  WARNING: runtime_environments_lookup.json not found at {rt_lookup_path} — name matching disabled")

    # Load encrypted values CSV if present
    enc_csv_data = load_encrypted_fields_csv(ENCRYPTED_FIELDS_CSV)
    if enc_csv_data:
        print(f"\n  Loaded encrypted fields CSV: {ENCRYPTED_FIELDS_CSV}")
        print(f"  {len(enc_csv_data)} connection(s) have values staged")
    else:
        print(f"\n  No encrypted_fields.csv found at {ENCRYPTED_FIELDS_CSV} — encrypted fields will need manual entry")

    if update_mode:
        print("\n  UPDATE MODE: existing connections will be updated with export payload")

    print()
    created  = 0
    updated  = 0
    skipped  = 0
    failed   = 0
    enc_log  = []  # list of (conn_name, field_path) for fields that still need values

    for fname in sorted(conn_files):
        fpath = os.path.join(CONN_DIR, fname)
        with open(fpath, encoding="utf-8") as f:
            conn = json.load(f)

        name         = conn.get("name", fname)
        source_rt_id = conn.get("runtimeEnvironmentId", "")
        source_rt_name = source_rt_id_to_name.get(source_rt_id, "")

        # Resolve runtime environment in target
        try:
            target_rt_id, target_rt_name, target_agent_id, match_reason = resolve_runtime_env(
                source_rt_name, source_rt_id, target_runtimes
            )
        except RuntimeError as e:
            print(f"  ERROR    : {name}  — {e}")
            failed += 1
            continue

        # Build clean payload — strip system fields, remap runtime/agent IDs
        enc = find_encrypted_fields(conn)
        payload = strip_fields(conn)
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
                print(f"  SKIP     : {name}  (already exists as {existing[name]})")
                skipped += 1
                continue
            # Update mode — POST to /connection/{id} with PARTIAL
            conn_id = existing[name]
            print(f"  UPDATING : {name}  ({conn_id})")
            print(f"             Runtime env  -> '{target_rt_name}' ({match_reason})")
            print(f"             Agent ID     -> {target_agent_id or 'none'}")
            resp = requests.post(
                f"{auth['iics_url']}/saas/api/v2/connection/{conn_id}",
                headers={**session_headers(auth), "Update-Mode": "PARTIAL"},
                json=payload,
                timeout=30,
            )
            if resp.ok:
                print(f"             Updated OK")
                updated += 1
                apply_encrypted_fields_for_connection(
                    auth, name, conn_id, enc_csv_data.get(name, {}))
                for fp in enc:
                    if fp not in enc_csv_data.get(name, {}):
                        enc_log.append((name, fp))
            else:
                print(f"             FAILED {resp.status_code}: {resp.text[:300]}")
                failed += 1
        else:
            # Create
            print(f"  CREATING : {name}")
            print(f"             Runtime env  -> '{target_rt_name}' ({match_reason})")
            print(f"             Agent ID     -> {target_agent_id or 'none'}")
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
                apply_encrypted_fields_for_connection(
                    auth, name, conn_id, enc_csv_data.get(name, {}))
                for fp in enc:
                    if fp not in enc_csv_data.get(name, {}):
                        enc_log.append((name, fp))
                if enc and not enc_csv_data.get(name):
                    print(f"             Encrypted fields need values: {', '.join(enc)}")
            else:
                print(f"             FAILED {resp.status_code}: {resp.text[:300]}")
                failed += 1

    # Write/overwrite encrypted_fields.csv with any fields still needing values
    if enc_log:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(EXPORT_DIR, f"import_encrypted_fields_report_{timestamp}.csv")
        try:
            with open(report_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Connection Name", "Parameter Name", "Value"])
                for conn_name, field_path in enc_log:
                    existing_val = enc_csv_data.get(conn_name, {}).get(field_path, "")
                    writer.writerow([conn_name, field_path, existing_val])
            enc_written = True
        except PermissionError:
            print(f"\n  WARNING: Could not write {report_path} — permission denied.")
            enc_written = False
    else:
        enc_written = False

    print("\n" + "=" * 60)
    print(f"  Created : {created}")
    if update_mode:
        print(f"  Updated : {updated}")
    print(f"  Skipped : {skipped}  (already existed)")
    print(f"  Failed  : {failed}")
    if enc_written:
        uniq = len({r[0] for r in enc_log})
        print(f"\n  Encrypted fields report written: {report_path}")
        print(f"  ({len(enc_log)} field(s) across {uniq} connection(s) still need values)")
    print("=" * 60)


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


def replace_masked_values(obj):
    """
    Recursively replace any '********' sentinel values with PLACEHOLDER
    so the POST is accepted by the API. Real values must be set afterwards
    using the encrypted_fields CSV.
    """
    if isinstance(obj, dict):
        return {k: replace_masked_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_masked_values(v) for v in obj]
    elif obj == MASKED_VALUE:
        return PLACEHOLDER
    return obj


# ---------------------------------------------------------------------------
# Main import logic
# ---------------------------------------------------------------------------

def apply_encrypted_fields(auth, csv_file):
    """
    Reads a filled-in encrypted_fields CSV and applies each value to its
    connection via a PARTIAL update POST to /saas/api/v2/connection/{id}.

    CSV format: Connection Name, Parameter Name, Value
    Parameter Name uses dot-notation: "password" or "connParams.SecretKey"
    Rows with empty Value are skipped.
    """
    print(f"\n{'=' * 60}")
    print(f"Applying encrypted fields from: {csv_file}")
    print(f"{'=' * 60}")

    # Fetch live connections from target org to get correct IDs
    print("  Fetching connections from target org...")
    resp = requests.get(
        f"{auth['iics_url']}/saas/api/v2/connection",
        headers=session_headers(auth),
        timeout=30,
    )
    resp.raise_for_status()
    conn_list = resp.json() if isinstance(resp.json(), list) else []
    conn_lookup = {c["name"]: c["id"] for c in conn_list if "name" in c and "id" in c}
    print(f"  Found {len(conn_lookup)} connection(s) in target org\n")

    # Read CSV and group fields by connection name
    updates = {}  # conn_name -> {field_path: value}
    skipped_empty = 0
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
        print(f"  No values to apply (all rows empty or CSV has no data).")
        if skipped_empty:
            print(f"  ({skipped_empty} row(s) skipped — Value column empty)")
        return

    print(f"  {len(updates)} connection(s) to update, {skipped_empty} row(s) skipped (empty value)\n")

    applied = 0
    failed  = 0

    for conn_name, fields in updates.items():
        conn_id = conn_lookup.get(conn_name)
        if not conn_id:
            print(f"  SKIP  : '{conn_name}' — not found in connections_lookup.json")
            failed += 1
            continue

        # Build PARTIAL payload from all fields for this connection
        payload = {"@type": "connection"}
        for field_path, value in fields.items():
            parts = field_path.split(".", 1)
            if len(parts) == 2:
                # Nested: e.g. connParams.SecretKey -> {"connParams": {"SecretKey": value}}
                parent, child = parts
                if parent not in payload:
                    payload[parent] = {}
                payload[parent][child] = value
            else:
                payload[field_path] = value

        url = f"{auth['iics_url']}/saas/api/v2/connection/{conn_id}"
        headers = {
            "Content-Type": "application/json",
            "Accept":       "application/json",
            "icSessionId":  auth["session_id"],
            "Update-Mode":  "PARTIAL",
        }

        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        field_list = ", ".join(fields.keys())

        if resp.ok:
            print(f"  UPDATED : {conn_name}  ({field_list})")
            applied += 1
        else:
            print(f"  FAILED  : {conn_name}  ({field_list})")
            print(f"            {resp.status_code}: {resp.text[:200]}")
            failed += 1

    print(f"\n{'=' * 60}")
    print(f"  Applied : {applied}")
    print(f"  Failed  : {failed}")
    print(f"{'=' * 60}")


def main():
    parser = argparse.ArgumentParser(description="CDGC Connection Import")
    parser.add_argument(
        "--encrypted-fields",
        metavar="CSV_FILE",
        help="Apply encrypted fields from a specific CSV file only (skip connection import)",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update existing connections with export payload (default: skip existing)",
    )
    args = parser.parse_args()

    pod_url = f"https://{POD}.informaticacloud.com"

    print("=" * 60)
    print("CDGC Connection Import")
    print("=" * 60)
    print(f"\nPOD      : {POD}")
    print(f"Pod URL  : {pod_url}")

    print("\n[Auth] Logging in...")
    auth = login(pod_url, USERNAME, PASSWORD)

    # Mode: apply encrypted fields CSV only
    if args.encrypted_fields:
        if not os.path.isfile(args.encrypted_fields):
            print(f"\nERROR: File not found: {args.encrypted_fields}")
            sys.exit(1)
        apply_encrypted_fields(auth, args.encrypted_fields)
        return

    # Mode: full import (with optional update)
    print(f"Source   : {CONN_DIR}\n")
    import_connections(auth, update_mode=args.update)


if __name__ == "__main__":
    main()
