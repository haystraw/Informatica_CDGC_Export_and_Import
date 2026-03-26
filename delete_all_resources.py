"""
delete_all_resources.py

Deletes ALL catalog sources (and optionally connections) in the target IDMC org.
Dry-run by default — pass --go to actually delete.
Catalog sources are always included. Pass --connections to also delete connections.

Usage:
  python delete_all_resources.py                   # dry run: catalog sources only
  python delete_all_resources.py --connections     # dry run: catalog sources + connections
  python delete_all_resources.py --go              # live delete: catalog sources only
  python delete_all_resources.py --connections --go # live delete: catalog sources + connections
"""

import os
import sys
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
P = "cdgc_delete_resources"
POD      = get(f"{P}_pod",      "dmp-us")
USERNAME = get(f"{P}_username", "your_username_here")
PASSWORD = get(f"{P}_password", "your_password_here")
CONFIRM  = get_bool(f"{P}_confirm", True)
# ---------------------------------------------------------------------------


def login(pod_url, username, password):
    resp = requests.post(
        f"{pod_url}/saas/public/core/v3/login",
        json={"username": username, "password": password},
        headers={"content-type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    data       = resp.json()
    user_info  = data.get("userInfo", {})
    session_id = user_info.get("sessionId")
    org_id     = user_info.get("orgId")
    org_name   = user_info.get("orgName", "")
    raw_base   = data.get("products", [{}])[0].get("baseApiUrl", "")
    iics_url   = raw_base[:-len("/saas")] if raw_base.endswith("/saas") else raw_base or pod_url

    if not session_id:
        raise RuntimeError(f"Login failed — no sessionId. Response: {data}")

    # JWT token for catalog source API
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

    cdgc_api_url = pod_url.replace("https://", "https://cdgc-api.")
    api_url      = pod_url.replace("https://", "https://idmc-api.")

    user_name = user_info.get("name", username)
    print(f"  Logged in as : {user_name}")
    print(f"  Org          : {org_name} ({org_id})")
    print(f"  IICS URL     : {iics_url}")
    print(f"  CDGC API URL : {cdgc_api_url}")

    return {
        "session_id":   session_id,
        "access_token": access_token,
        "org_id":       org_id,
        "org_name":     org_name,
        "user_name":    user_name,
        "iics_url":     iics_url,
        "cdgc_api_url": cdgc_api_url,
        "api_url":      api_url,
    }


def session_headers(auth):
    return {
        "Content-Type":    "application/json",
        "Accept":          "application/json",
        "INFA-SESSION-ID": auth["session_id"],
        "IDS-SESSION-ID":  auth["session_id"],
        "icSessionId":     auth["session_id"],
    }


def cdgc_headers(auth):
    return {
        "X-INFA-ORG-ID": auth["org_id"],
        "IDS-SESSION-ID": auth["session_id"],
        "Authorization":  f"Bearer {auth['access_token']}",
        "Content-Type":   "application/json",
    }


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

def get_all_connections(auth):
    resp = requests.get(
        f"{auth['iics_url']}/saas/api/v2/connection",
        headers=session_headers(auth),
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def delete_connections(auth, dry_run):
    print("\n[Connections]")
    print("-" * 40)
    connections = get_all_connections(auth)
    print(f"  Found {len(connections)} connection(s)\n")

    if not connections:
        print("  Nothing to delete.")
        return 0

    for conn in connections:
        print(f"  {'[DRY RUN] ' if dry_run else ''}DELETE : {conn.get('name')}  ({conn.get('id')})")

    if dry_run:
        print(f"\n  {len(connections)} connection(s) would be deleted.")
        return len(connections)

    deleted, failed = 0, 0
    print()
    for conn in connections:
        name = conn.get("name")
        cid  = conn.get("id")
        resp = requests.delete(
            f"{auth['iics_url']}/saas/api/v2/connection/{cid}",
            headers=session_headers(auth),
            timeout=30,
        )
        if resp.ok or resp.status_code == 404:
            print(f"  DELETED : {name}  ({cid})")
            deleted += 1
        else:
            print(f"  FAILED  : {name}  ({cid})  {resp.status_code}: {resp.text[:150]}")
            failed += 1

    print(f"\n  Deleted: {deleted}  Failed: {failed}")
    return deleted


# ---------------------------------------------------------------------------
# Catalog sources
# ---------------------------------------------------------------------------

def get_all_catalog_sources(auth):
    endpoint = f"{auth['cdgc_api_url']}/ccgf-catalog-source-management/api/v1/datasources"
    offset, limit, results = 0, 50, []
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
        results.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return results


def delete_catalog_sources(auth, dry_run):
    print("\n[Catalog Sources]")
    print("-" * 40)
    sources = get_all_catalog_sources(auth)
    print(f"  Found {len(sources)} catalog source(s)\n")

    if not sources:
        print("  Nothing to delete.")
        return 0

    for cs in sources:
        print(f"  {'[DRY RUN] ' if dry_run else ''}DELETE : {cs.get('name')}  ({cs.get('id')})")

    if dry_run:
        print(f"\n  {len(sources)} catalog source(s) would be deleted.")
        return len(sources)

    endpoint = f"{auth['api_url']}/data360/catalog-source-management/v1/catalogsources"
    deleted, failed = 0, 0
    print()
    for cs in sources:
        name  = cs.get("name")
        cs_id = cs.get("id")
        resp  = requests.delete(
            f"{endpoint}/{cs_id}",
            headers=cdgc_headers(auth),
            params={"type": "delete"},
            timeout=30,
        )
        if resp.ok or resp.status_code == 404:
            print(f"  DELETED : {name}  ({cs_id})")
            deleted += 1
        else:
            print(f"  FAILED  : {name}  ({cs_id})  {resp.status_code}: {resp.text[:150]}")
            failed += 1

    print(f"\n  Deleted: {deleted}  Failed: {failed}")
    return deleted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global POD, USERNAME, PASSWORD, CONFIRM

    parser = argparse.ArgumentParser(description="Delete all catalog sources and/or connections")
    parser.add_argument(f"--{P}_pod",      default=POD,      metavar="POD",  dest="pod")
    parser.add_argument(f"--{P}_username", default=USERNAME, metavar="USER", dest="username")
    parser.add_argument(f"--{P}_password", default=PASSWORD, metavar="PASS", dest="password")
    parser.add_argument(f"--{P}_no_confirm", action="store_true", dest="no_confirm")
    parser.add_argument("--go",              action="store_true", help="Actually delete (default is dry run)")
    parser.add_argument("--connections",     action="store_true", help="Also delete connections (default: catalog sources only)")
    parser.add_argument("--catalog-sources", action="store_true", help="Delete catalog sources (included by default)")
    args = parser.parse_args()

    POD      = args.pod
    USERNAME = args.username
    PASSWORD = args.password
    if args.no_confirm:
        CONFIRM = False
    pod_url  = f"https://{POD}.informaticacloud.com"

    run_catalog_sources = True
    run_connections     = args.connections
    dry_run = not args.go

    print("=" * 60)
    print("CDGC Delete All Resources")
    print("=" * 60)
    print(f"\nPOD      : {POD}")
    print(f"Mode     : {'DRY RUN (pass --go to actually delete)' if dry_run else '*** LIVE DELETE ***'}")
    print(f"Scope    : {'connections + catalog sources' if run_connections and run_catalog_sources else 'connections only' if run_connections else 'catalog sources only'}")

    print("\n[Auth] Logging in...")
    auth = login(pod_url, USERNAME, PASSWORD)

    # Always list what would be affected first
    if run_catalog_sources:
        delete_catalog_sources(auth, dry_run=True)

    if run_connections:
        delete_connections(auth, dry_run=True)

    if dry_run:
        print("\n  Re-run with --go to actually delete.")
        return

    # Confirmation before live deletion
    scope = ("connections + catalog sources" if run_connections and run_catalog_sources
             else "connections only" if run_connections
             else "catalog sources only")
    if CONFIRM:
        print(f"\n{'=' * 60}")
        print(f"  WARNING: About to permanently delete {scope}")
        print(f"  Logged in as : {auth['user_name']}")
        print(f"  Org          : {auth['org_name']} ({auth['org_id']})")
        print(f"{'=' * 60}")
        confirm = input("  Type YES to confirm: ").strip()
        if confirm != "YES":
            print("  Aborted.")
            return
    else:
        print(f"\n  Confirmation disabled; proceeding to delete {scope}.")

    print()
    if run_catalog_sources:
        delete_catalog_sources(auth, dry_run=False)

    if run_connections:
        delete_connections(auth, dry_run=False)

    print("\n" + "=" * 60)
    print("Delete requests submitted.")
    print("NOTE: Deletions are processed asynchronously — resources may")
    print("      take additional time to fully disappear from the org.")
    print("=" * 60)


if __name__ == "__main__":
    main()
