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
# Configuration
# ---------------------------------------------------------------------------
P = "cdgc_delete_connections"
POD      = get(f"{P}_pod",      "dmp-us")
USERNAME = get(f"{P}_username", "your_username_here")
PASSWORD = get(f"{P}_password", "your_password_here")
CONFIRM  = get_bool(f"{P}_confirm", True)
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
    org_name   = user_info.get("orgName", "")
    raw_base   = data.get("products", [{}])[0].get("baseApiUrl", "")
    iics_url   = raw_base[:-len("/saas")] if raw_base.endswith("/saas") else raw_base
    if not session_id:
        raise RuntimeError(f"Login failed — no sessionId. Response: {data}")
    return {"session_id": session_id, "org_id": org_id, "org_name": org_name, "iics_url": iics_url}


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
    global POD, USERNAME, PASSWORD, POD_URL, CONFIRM

    parser = argparse.ArgumentParser(description="Delete all connections in an IDMC org")
    parser.add_argument(f"--{P}_pod",      default=POD,      metavar="POD",  dest="pod")
    parser.add_argument(f"--{P}_username", default=USERNAME, metavar="USER", dest="username")
    parser.add_argument(f"--{P}_password", default=PASSWORD, metavar="PASS", dest="password")
    parser.add_argument(f"--{P}_no_confirm", action="store_true", dest="no_confirm")
    parser.add_argument("--go", action="store_true")
    args = parser.parse_args()

    POD      = args.pod
    USERNAME = args.username
    PASSWORD = args.password
    if args.no_confirm:
        CONFIRM = False
    POD_URL  = f"https://{POD}.informaticacloud.com"

    dry_run = not args.go

    print("=" * 60)
    print("CDGC Delete All Connections")
    print("=" * 60)
    print(f"\nPOD      : {POD}")
    print(f"Mode     : {'DRY RUN (pass --go to actually delete)' if dry_run else '*** LIVE DELETE ***'}\n")

    print("[Auth] Logging in...")
    auth = login(POD_URL, USERNAME, PASSWORD)
    print(f"  Org      : {auth.get('org_name', '')} ({auth['org_id']})")
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
    if CONFIRM:
        print(f"\n  WARNING: This will permanently delete {len(connections)} connection(s) from org {auth['org_id']}.")
        confirm = input("  Type YES to confirm: ").strip()
        if confirm != "YES":
            print("  Aborted.")
            return
    else:
        print(f"\n  Confirmation disabled; deleting {len(connections)} connection(s).")

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
