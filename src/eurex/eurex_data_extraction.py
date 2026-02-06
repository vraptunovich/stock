"""
src/eurex/eurex_data_extraction.py

Fetch FlexibleContracts and Contracts for multiple products from DBP/Eurex GraphQL.

Auth (REQUIRED):
  X-DBP-APIKEY: <apikey>

Config (YAML):
  config/eurex_config.yaml (default)
  Expected keys:
    graphql:
      url: "https://api.developer.deutsche-boerse.com/eurex-prod-graphql/"   # note trailing slash is often required
      apikey: "YOUR_KEY"
    products:
      - "NESN"
      - ...
    queries:
      flexible_contracts: |
        ...
      contracts: |
        ...

Extras:
  - Logs masked request headers (confirms X-DBP-APIKEY presence)
  - Logs response headers and body preview on HTTP errors
  - Optional --ping (query { __typename }) to validate connectivity/auth
  - Auto URL fallback on 404 (tries with/without trailing slash once)
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Optional, Tuple

import requests

from src.helper import load_config

logger = logging.getLogger("eurex_graphql_fetch")


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def require(cfg: dict, dotted_path: str) -> Any:
    cur: Any = cfg
    for key in dotted_path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            raise KeyError(f"Missing config key: {dotted_path}")
        cur = cur[key]
    return cur


def mask_apikey(value: Optional[str]) -> str:
    if not value:
        return "<missing>"
    v = str(value)
    if len(v) <= 8:
        return "***"
    return f"{v[:4]}...{v[-4:]}"


def url_variants(url: str) -> Tuple[str, str]:
    """
    Returns (as_given, toggled_trailing_slash) variants.
    If url endswith '/', toggled is without it; else toggled is with it.
    """
    u = url.strip()
    if u.endswith("/"):
        return u, u.rstrip("/")
    return u, u + "/"


class GraphQLClient:
    def __init__(self, config: dict):
        graphql_cfg = require(config, "graphql")

        self.url_raw: str = str(require(config, "graphql.url")).strip()
        apikey = graphql_cfg.get("apikey")

        # Enforce X-DBP-APIKEY usage
        if not apikey or not str(apikey).strip():
            raise ValueError(
                "Missing config key: graphql.apikey. "
                "This API requires X-DBP-APIKEY header."
            )
        self.apikey: str = str(apikey).strip()

        self.products: list[str] = config.get("products", []) or []
        self.queries: dict[str, str] = require(config, "queries")

        # Validate expected queries exist
        _ = require(config, "queries.contracts")
        _ = require(config, "queries.flexible_contracts")

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-DBP-APIKEY": self.apikey,
            }
        )

        primary, toggled = url_variants(self.url_raw)
        self.url_primary = primary
        self.url_fallback = toggled

        logger.info("GraphQL URL (primary): %s", self.url_primary)
        logger.info("GraphQL URL (fallback): %s", self.url_fallback)
        logger.info("X-DBP-APIKEY: %s", mask_apikey(self.apikey))

    def _masked_headers_for_log(self) -> dict[str, str]:
        masked = dict(self.session.headers)
        if "X-DBP-APIKEY" in masked:
            masked["X-DBP-APIKEY"] = mask_apikey(masked["X-DBP-APIKEY"])
        return {k: str(v) for k, v in masked.items()}

    def _post(self, url: str, payload: dict[str, Any]) -> requests.Response:
        logger.debug("Request headers (masked): %s", self._masked_headers_for_log())
        logger.debug("POST %s payload_keys=%s", url, list(payload.keys()))
        return self.session.post(url, json=payload, timeout=60)

    def request(
            self,
            query: str,
            variables: dict[str, Any],
            *,
            operation_name: str = "",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"query": query, "variables": variables}
        if operation_name:
            payload["operationName"] = operation_name

        # 1) Try primary URL
        r = self._post(self.url_primary, payload)

        # If 404, try fallback URL once (toggle trailing slash)
        if r.status_code == 404 and self.url_fallback != self.url_primary:
            logger.warning(
                "Received 404 from primary URL. Retrying with fallback URL: %s",
                self.url_fallback,
            )
            r = self._post(self.url_fallback, payload)

        # HTTP error handling with diagnostics
        if r.status_code >= 400:
            body_preview = (r.text or "")[:2000]
            logger.error(
                "HTTP error: status=%s url=%s resp_headers=%s body=%s",
                r.status_code,
                r.url,
                dict(r.headers),
                body_preview,
            )
            r.raise_for_status()

        data = r.json()

        # GraphQL-level errors (can still come with 200)
        if data.get("errors"):
            logger.error("GraphQL errors: %s", data["errors"])
            raise RuntimeError(f"GraphQL errors: {data['errors']}")

        return data.get("data", {})

    def ping(self) -> dict[str, Any]:
        q = "query Ping { __typename }"
        return self.request(q, {}, operation_name="Ping")

    def fetch_flexible_contracts(self, product: str) -> dict[str, Any]:
        q = self.queries["flexible_contracts"]
        data = self.request(q, {"product": product}, operation_name="FlexibleContractsByProductAndDate")
        return data["FlexibleContracts"]

    def fetch_contracts(self, product: str) -> dict[str, Any]:
        q = self.queries["contracts"]
        data = self.request(q, {"product": product}, operation_name="AllContracts")
        return data["Contracts"]


def fetch_all(client: GraphQLClient) -> dict[str, Any]:
    results: dict[str, Any] = {}

    if not client.products:
        logger.warning("No products defined in config (key: products). Nothing to fetch.")
        return results

    for product in client.products:
        logger.info("Fetching product=%s", product)

        flexible = client.fetch_flexible_contracts(product)
        contracts = client.fetch_contracts(product)

        results[product] = {
            "flexible_contracts": flexible,  # {date, data:[...]}
            "contracts": contracts,          # {date, data:[...]}
        }

        logger.info(
            "%s fetched: flexible_rows=%d contracts_rows=%d",
            product,
            len(flexible.get("data", []) or []),
            len(contracts.get("data", []) or []),
        )

    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default="config/eurex_config.yaml",
        help="Path to config YAML (default: config/eurex_config.yaml)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Optional output JSON file path. If provided, saves full response payloads.",
    )
    parser.add_argument(
        "--ping",
        action="store_true",
        help="Run a minimal Ping query first (query { __typename }) to validate connectivity/auth.",
    )
    args = parser.parse_args()

    setup_logging(args.log_level)

    config = load_config(args.config)
    client = GraphQLClient(config)

    if args.ping:
        logger.info("Running ping query...")
        ping_res = client.ping()
        logger.info("Ping OK: %s", ping_res)

    results = fetch_all(client)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Saved results to %s", out_path)

    # Short summary
    for product, payload in results.items():
        flex_n = len(payload["flexible_contracts"].get("data", []) or [])
        con_n = len(payload["contracts"].get("data", []) or [])
        print(f"{product}: FlexibleContracts={flex_n}, Contracts={con_n}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
