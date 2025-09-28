"""
get_inventory.py

Reads configuration from environment variables (optionally .env), authenticates to
WMS to obtain a session token, then fetches inventory data and saves it to
`inventory_wms.json` in the current directory.

Required env variables:
- WMS_PROD_URL: Authentication URL (login endpoint)
- WMS_API_KEY: API key for login
- WMS_SECRET: API secret for login
- WMS_LIST_INV: Inventory listing URL

Optional env variables:
- WMS_LIST_INV_METHOD: HTTP method for inventory request (GET or POST). Default: POST
- WMS_AUTH_HEADER_PREFIX: Prefix for Authorization header (e.g., "Bearer "). Default: ""

Usage:
  python3 get_inventory.py
"""

import json
import os
import sys
from typing import Any, Dict, Optional

import requests


def try_load_dotenv() -> None:
    """Load variables from a .env file if python-dotenv is available."""
    try:
        # Imported lazily to avoid hard dependency
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(override=False)
    except Exception:
        # Silently ignore if dotenv is not installed
        pass


def get_env(name: str, required: bool = True, default: Optional[str] = None) -> str:
    """Fetch an environment variable, optionally required.

    Raises a descriptive error if a required variable is missing.
    """
    value = os.getenv(name, default)
    if required and (value is None or str(value).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return str(value) if value is not None else ""


def _normalize_url(url: str) -> str:
    """Ensure URL has a scheme; default to https if missing."""
    trimmed = url.strip()
    if not trimmed.lower().startswith(("http://", "https://")):
        return f"https://{trimmed}"
    return trimmed


def authenticate(auth_url: str, api_key: str, api_secret: str) -> str:
    """Authenticate to WMS and return session token.

    Expects the API to respond with JSON containing `data.session_token` on success.
    """
    payload: Dict[str, Any] = {"api_key": api_key, "api_secret": api_secret}
    headers = {"Content-Type": "application/json"}
    response = requests.post(_normalize_url(auth_url), json=payload, headers=headers, timeout=60)
    response.raise_for_status()
    body = response.json()

    if not isinstance(body, dict) or "data" not in body or not isinstance(body["data"], dict):
        raise RuntimeError("Unexpected auth response format: missing data object")
    session_token = body["data"].get("session_token")
    if not session_token:
        raise RuntimeError("Authentication succeeded but no session_token returned")
    return str(session_token)


def fetch_inventory(list_url: str, token: str, method: str, auth_prefix: str) -> Dict[str, Any]:
    """Fetch inventory list using the provided session token.

    Method can be GET or POST. Default is POST with empty JSON payload.
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"{auth_prefix}{token}",
    }
    method_upper = method.strip().upper()
    normalized_url = _normalize_url(list_url)

    def _do_request(http_method: str) -> Dict[str, Any]:
        if http_method == "GET":
            resp = requests.get(normalized_url, headers=headers, timeout=120)
        else:
            resp = requests.post(normalized_url, json={}, headers=headers, timeout=120)
        resp.raise_for_status()
        return resp.json()

    # First attempt with configured method
    result = _do_request(method_upper)
    data_field = result.get("data") if isinstance(result, dict) else None

    # If data is missing/None/not list, try the alternate method once
    if (not isinstance(data_field, list)):
        alt_method = "GET" if method_upper != "GET" else "POST"
        try:
            print(f"Primary fetch with {method_upper} did not return list data; retrying with {alt_method}...")
            result_alt = _do_request(alt_method)
            data_alt = result_alt.get("data") if isinstance(result_alt, dict) else None
            if isinstance(data_alt, list) and len(data_alt) >= 0:
                return result_alt
        except Exception:
            # Fall through to return original result if alternate fails
            pass

    return result


def save_json_file(content: Dict[str, Any], output_path: str) -> None:
    """Save a JSON-serializable object to a file with pretty formatting."""
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(content, f, ensure_ascii=False, indent=2)


def _normalize_optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return None if s == "" or s.lower() == "nan" else s


def _normalize_pack_id(value: Any) -> Optional[str]:
    text = _normalize_optional_text(value)
    if text is None:
        return None
    if text.isdigit() and len(text) < 4:
        return text.zfill(4)
    return text


def _normalize_product_id(value: Any) -> Optional[str]:
    text = _normalize_optional_text(value)
    if text is None:
        return None
    if text.endswith(".0") and text.replace(".", "", 1).isdigit():
        return text[:-2]
    return text


def _normalize_conversion_list(value: Any) -> Any:
    if value is None:
        return []
    if not isinstance(value, list):
        return []
    normalized: list[Dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        uom = _normalize_optional_text(item.get("uom"))
        numerator_raw = item.get("numerator")
        denominator_raw = item.get("denominator")
        barcode = _normalize_optional_text(item.get("barcode"))
        qty_raw = item.get("qty")
        try:
            numerator_val = int(numerator_raw) if numerator_raw is not None else None
        except Exception:
            numerator_val = None
        try:
            denominator_val = int(denominator_raw) if denominator_raw is not None else None
        except Exception:
            denominator_val = None
        try:
            qty_val = float(qty_raw) if qty_raw is not None else None
        except Exception:
            qty_val = None
        normalized.append({
            "uom": uom,
            "numerator": numerator_val,
            "denominator": denominator_val,
            "barcode": barcode,
            "qty": qty_val,
        })
    return normalized


def _norm_key(s: str) -> str:
    return s.replace("_", "").replace("-", "").lower()


def _get_by_fuzzy_keys(rec: Dict[str, Any], candidates: list[str]) -> Any:
    """Return the first matching value by fuzzy key match (case/underscore-insensitive)."""
    if not isinstance(rec, dict):
        return None
    normalized_map = {_norm_key(k): k for k in rec.keys()}
    for cand in candidates:
        key_norm = _norm_key(cand)
        if key_norm in normalized_map:
            return rec.get(normalized_map[key_norm])
    return None


def normalize_inventory_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure each record contains expected keys and normalized types.

    If the API omits some keys (e.g., group_description, pack_id), they will be
    added with None to keep the schema consistent with the expected example.
    """
    if not isinstance(payload, dict):
        return payload
    data = payload.get("data")
    if not isinstance(data, list):
        return payload

    expected_keys = [
        "client_id",
        "warehouse_id",
        "product_id",
        "product_description",
        "batch",
        "pack_id",
        "expired_date",
        "base_qty",
        "base_uom",
        "stock_type",
        "gross_weight",
        "volume",
        "group_description",
        "conversion",
    ]

    normalized_records: list[Dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue
        new_rec: Dict[str, Any] = {k: rec.get(k) for k in expected_keys if k in rec}

        # Ensure keys exist
        for k in expected_keys:
            if k not in new_rec:
                new_rec[k] = None if k != "conversion" else []

        # Field-specific normalization
        new_rec["product_id"] = _normalize_product_id(
            _get_by_fuzzy_keys(rec, ["product_id", "productid", "sku", "productId"])
        )
        pack_id_raw = _get_by_fuzzy_keys(rec, ["pack_id", "packid", "packId", "pack_code", "packcode", "pack"])
        new_rec["pack_id"] = _normalize_pack_id(pack_id_raw)
        new_rec["base_uom"] = _normalize_optional_text(rec.get("base_uom"))
        new_rec["stock_type"] = _normalize_optional_text(rec.get("stock_type"))
        group_desc_raw = _get_by_fuzzy_keys(rec, [
            "group_description",
            "group_desc",
            "groupDescription",
            "group",
            "product_group_description",
            "productGroupDescription",
        ])
        new_rec["group_description"] = _normalize_optional_text(group_desc_raw)
        new_rec["product_description"] = _normalize_optional_text(rec.get("product_description"))
        new_rec["batch"] = _normalize_optional_text(rec.get("batch"))
        new_rec["expired_date"] = _normalize_optional_text(rec.get("expired_date"))

        # numeric safe-casts (keep original if cast fails)
        def _to_float(v: Any) -> Optional[float]:
            try:
                return float(v) if v is not None else None
            except Exception:
                return None

        new_rec["base_qty"] = _to_float(rec.get("base_qty"))
        new_rec["gross_weight"] = _to_float(rec.get("gross_weight"))
        new_rec["volume"] = _to_float(rec.get("volume"))

        new_rec["conversion"] = _normalize_conversion_list(rec.get("conversion"))

        normalized_records.append(new_rec)

    payload["data"] = normalized_records
    return payload


def main() -> int:
    try_load_dotenv()

    try:
        auth_url = get_env("WMS_PROD_URL")
        api_key = get_env("WMS_API_KEY")
        api_secret = get_env("WMS_SECRET")
        list_url = get_env("WMS_LIST_INV")
        method = get_env("WMS_LIST_INV_METHOD", required=False, default="POST")
        auth_prefix = get_env("WMS_AUTH_HEADER_PREFIX", required=False, default="")

        print("Authenticating to WMS...")
        token = authenticate(auth_url=auth_url, api_key=api_key, api_secret=api_secret)
        print("Authentication successful. Session token obtained.")

        print("Fetching inventory list...")
        inventory_json = fetch_inventory(list_url=list_url, token=token, method=method, auth_prefix=auth_prefix)
        inventory_json = normalize_inventory_payload(inventory_json)

        output_file = os.path.join(os.getcwd(), "inventory_wms.json")
        save_json_file(inventory_json, output_file)

        total_count = inventory_json.get("total_count")
        record_count = inventory_json.get("record_count")
        print("Inventory fetched and saved.")
        if total_count is not None or record_count is not None:
            print(f"total_count={total_count}, record_count={record_count}")
        print(f"Output file: {output_file}")
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())


