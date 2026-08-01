"""Network guardrail: this app must never route traffic through Bloomberg-internal
proxy infrastructure (personal hackathon project, not company infra).

`clickhouse-connect` and `urllib` both honor the standard `*_PROXY` environment
variables. If the host shell has a Bloomberg proxy configured (e.g. for other,
work-related tools), it leaks into this app's outbound calls to ClickHouse Cloud
and LibreChat too — which fails whenever that proxy host isn't resolvable
(e.g. bbvpn off). Strip any proxy env var pointing at a bloomberg.com host as
soon as this module is imported, before any HTTP client is constructed.

Import this first, before clickhouse_connect / urllib / requests are used.
"""

from __future__ import annotations

import os

_PROXY_VARS = (
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
)

for _var in _PROXY_VARS:
    _value = os.environ.get(_var, "")
    if "bloomberg" in _value.lower():
        del os.environ[_var]
