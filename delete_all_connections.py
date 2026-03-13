"""
delete_all_connections.py

Deletes ALL connections in the target IDMC org.
Includes a confirmation prompt and a dry-run mode.

Usage:
  python delete_all_connections.py           # dry run (default, safe)
  python delete_all_connections.py --go      # actually delete
"""

import os
import sys
import json
import argparse
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
POD           = "dmp-us"                      # e.g. dmp-us
USERNAME      = "shayes_compass_debug"
PASSWORD = "Infa2025c!"          # your IDMC login password
# ---------------------------------------------------------------------------

POD_URL = f"https://{POD}.informaticacloud.com"


def login(pod_url, username, password):
    resp = requests.post(
        f"{pod_url}/saas/public/core/v3/login",
        json={"username": username, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    data       = resp.json()
    user_info  = data.get("userInfo", {})
    session_id = user_info.get("sessionId")
    org_id     = user_info.get("orgId")
    raw_base   = data.get("products", [{}])[0].get("baseApiUrl", "")
    iics_url   = raw_base[:-len("/saas")] if raw_base.endswith("/saas") else raw_base
    if not session_id:
        raise RuntimeError(f"Login failed — no sessionId. Response: {data}")
    return {"session_id": session_id, "org_id": org_id, "iics_url": iics_url}


def session_headers(auth):
    return {
        "Content-Type": "application/json",
        "Accept":       "application/json",
        "icSessionId":  auth["session_id"],
    }


def get_all_connections(auth):
    resp = requests.get(
        f"{auth['iics_url']}/saas/api/v2/connection",
        headers=session_headers(auth),
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def delete_connection(auth, conn_id):
    resp = requests.delete(
        f"{auth['iics_url']}/saas/api/v2/connection/{conn_id}",
        headers=session_headers(auth),
        timeout=30,
    )
    return resp


def main():
    parser = argparse.ArgumentParser(description="Delete all connections in an IDMC org")
    parser.add_argument(
        "--go",
        action="store_true",
        help="Actually perform deletions (default is dry run)",
    )
    args = parser.parse_args()
    dry_run = not args.go

    print("=" * 60)
    print("CDGC Delete All Connections")
    print("=" * 60)
    print(f"\nPOD      : {POD}")
    print(f"Mode     : {'DRY RUN (pass --go to actually delete)' if dry_run else '*** LIVE DELETE ***'}\n")

    print("[Auth] Logging in...")
    auth = login(POD_URL, USERNAME, PASSWORD)
    print(f"  Org ID   : {auth['org_id']}")
    print(f"  IICS URL : {auth['iics_url']}\n")

    print("Fetching connections...")
    connections = get_all_connections(auth)
    print(f"  Found {len(connections)} connection(s)\n")

    if not connections:
        print("Nothing to delete.")
        return

    # List what will be deleted
    for conn in connections:
        print(f"  {'[DRY RUN] ' if dry_run else ''}DELETE : {conn.get('name')}  ({conn.get('id')})")

    if dry_run:
        print(f"\n  Dry run complete — {len(connections)} connection(s) would be deleted.")
        print("  Re-run with --go to actually delete.")
        return

    # Confirmation prompt
    print(f"\n  WARNING: This will permanently delete {len(connections)} connection(s) from org {auth['org_id']}.")
    confirm = input("  Type YES to confirm: ").strip()
    if confirm != "YES":
        print("  Aborted.")
        return

    print()
    deleted = 0
    failed  = 0

    for conn in connections:
        name  = conn.get("name")
        cid   = conn.get("id")
        resp  = delete_connection(auth, cid)
        if resp.ok or resp.status_code == 404:
            print(f"  DELETED : {name}  ({cid})")
            deleted += 1
        else:
            print(f"  FAILED  : {name}  ({cid})  {resp.status_code}: {resp.text[:150]}")
            failed += 1

    print(f"\n{'=' * 60}")
    print(f"  Deleted : {deleted}")
    print(f"  Failed  : {failed}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
