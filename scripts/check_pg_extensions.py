"""
    Usage as a script:

    (while cf oauth-token && sleep 2m ; do : ; done) | python check_pg_extensions.py https://my.api.base.url.example.com service_guid extension_sets.csv

    This script generally takes longer to run than a single oauth token lasts
    so it continually polls for new tokens being fed in to its stdin, which
    can be provided by a looping `cf oauth-token` running every few minutes.

    The extension_sets csv is expected to have one set of extension names
    per row, one extension per column, in expected order of insertion.
    Really it's used as more of a list of lists than a table.

    Depends on requests. Not fussy about version.
"""

import csv
import json
from select import select
import sys
from time import sleep
from urllib.parse import urljoin

import requests


_auth_header = None
def _get_auth_header(f=sys.stdin):
    global _auth_header
    if _auth_header is None or select([f], [], [], 0)[0]:
        while True:
            line = next(f).strip()
            if line:
                print("<new auth token>", end="")
                sys.stdout.flush()
                _auth_header = {
                    "Authorization": line,
                }

            if _auth_header is not None:
                break

    return _auth_header


def _wait_for_success(api_base_url, service_guid):
    while True:
        sleep(10)
        resp = requests.get(
            urljoin(api_base_url, f"/v3/service_instances/{service_guid}"),
            headers=_get_auth_header(),
        )
        resp.raise_for_status()
        last_operation_state = resp.json()["last_operation"]["state"]
        if last_operation_state == "succeeded":
            print("succeeded")
            sys.stdout.flush()
            return
        elif last_operation_state == "failed":
            print(f"description: {resp.json()['last_operation']['description']}")
            sys.exit(8)
        print(".", end="")
        sys.stdout.flush()


def test_extensions(api_base_url, service_guid, extension_sets):
    for extension_set in extension_sets:
        for extension in extension_set:
            print(f"enabling extension {extension!r}", end="")
            sys.stdout.flush()
            resp = requests.patch(
                urljoin(api_base_url, f"/v3/service_instances/{service_guid}"),
                headers=_get_auth_header(),
                json={
                    "parameters": {
                        "enable_extensions": [extension],
                        "reboot": True,
                    },
                },
            )
            resp.raise_for_status()
            _wait_for_success(api_base_url, service_guid)

        for extension in reversed(extension_set):
            print(f"disabling extension {extension!r}", end="")
            sys.stdout.flush()
            resp = requests.patch(
                urljoin(api_base_url, f"/v3/service_instances/{service_guid}"),
                headers=_get_auth_header(),
                json={
                    "parameters": {
                        "disable_extensions": [extension],
                        "reboot": True,
                    },
                }
            )
            resp.raise_for_status()
            _wait_for_success(api_base_url, service_guid)


if __name__ == "__main__":
    api_base_url = sys.argv[1]
    service_guid = sys.argv[2]
    extension_sets_fn = sys.argv[3]
    with open(extension_sets_fn, "r") as f:
        extension_sets = list(csv.reader(f))

    test_extensions(api_base_url, service_guid, extension_sets)
