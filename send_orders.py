import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import requests
from dotenv import load_dotenv


# Load environment variables from .env if present
load_dotenv()

# URLs can be configured via .env
AUTH_URL = os.getenv(
    "AUTH_URL",
    "https://user-armos-preprod.applogicnesia.online/api/auth/login",
)
FEED_ORDER_URL = os.getenv(
    "FEED_ORDER_URL",
    "https://integration-armos-preprod.applogicnesia.online/api/webhook/v2/feed-order",
)


def excel_serial_to_iso(date_value: Union[int, float, str, None]) -> Optional[str]:
    """
    Convert Excel serial date (as seen in sample JSON) to ISO YYYY-MM-DD.
    - If already a string in ISO-like format, return as-is.
    - If None or empty, return None.
    Excel on Windows uses 1900-based serial with the leap year bug; pandas typically
    decodes to ints like 45917. We'll convert using 1899-12-30 origin.
    """
    if date_value is None:
        return None
    if isinstance(date_value, str):
        s = date_value.strip()
        if not s:
            return None
        # If looks like YYYY-MM-DD, pass through
        try:
            dt = datetime.fromisoformat(s)
            return dt.date().isoformat()
        except ValueError:
            # Not ISO format; attempt to parse common formats
            for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y", "%m-%d-%Y"):
                try:
                    return datetime.strptime(s, fmt).date().isoformat()
                except ValueError:
                    pass
            # Fallback: cannot parse
            return s
    # Assume numeric Excel serial
    try:
        base = datetime(1899, 12, 30)
        delta = timedelta(days=int(float(date_value)))
        return (base + delta).date().isoformat()
    except Exception:
        return None


def login_get_token(username: str, password: str, timeout: int = 30) -> str:
    resp = requests.post(
        AUTH_URL,
        json={"username": username, "password": password},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data or not data.get("success"):
        raise RuntimeError(f"Login failed: {data}")
    token = data.get("data", {}).get("token")
    if not token:
        raise RuntimeError("Login response missing token")
    return token


def build_payload(order: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        "warehouse_id": order.get("warehouse_id"),
        "client_id": order.get("client_id"),
        "outbound_reference": order.get("outbound_reference"),
        "divisi": order.get("divisi"),
        "faktur_date": excel_serial_to_iso(order.get("faktur_date")),
        "request_delivery_date": excel_serial_to_iso(order.get("request_delivery_date")),
        "origin_name": order.get("origin_name"),
        "origin_address_1": order.get("origin_address_1") or "",
        "origin_address_2": order.get("origin_address_2") or "",
        "origin_city": order.get("origin_city") or "",
        "origin_phone": order.get("origin_phone") or "",
        "origin_email": order.get("origin_email") or "",
        "destination_id": order.get("destination_id"),
        "destination_name": order.get("destination_name"),
        "destination_address_1": order.get("destination_address_1") or "",
        "destination_address_2": order.get("destination_address_2") or "",
        "destination_city": order.get("destination_city") or "",
        "destination_zip_code": order.get("destination_zip_code") or "",
        "destination_phone": order.get("destination_phone") or "",
        "destination_email": order.get("destination_email") or "",
        "order_type": order.get("order_type"),
        "items": [],
    }

    items = order.get("items") or []
    for item in items:
        # Ensure field types as strings where sample shows quoted numbers
        item_payload = {
            "warehouse_id": order.get("warehouse_id"),
            "line_id": str(item.get("line_id")) if item.get("line_id") is not None else "",
            "product_id": str(item.get("product_id")) if item.get("product_id") is not None else "",
            "product_description": item.get("product_description") or "",
            "group_id": str(item.get("group_id")) if item.get("group_id") is not None else "",
            "group_description": item.get("group_description") or "",
            "product_type": str(item.get("product_type")).zfill(3) if item.get("product_type") is not None else "",
            "qty": item.get("qty") or 0,
            "uom": item.get("uom") or "",
            "pack_id": str(item.get("pack_id")) if item.get("pack_id") is not None else "",
            "product_net_price": item.get("product_net_price") or 0,
            "conversion": [],
            "image_url": [""],
        }

        for conv in item.get("conversion") or []:
            item_payload["conversion"].append({
                "uom": conv.get("uom") or "",
                "numerator": int(conv.get("numerator") or 0),
                "denominator": int(conv.get("denominator") or 1),
            })

        payload["items"].append(item_payload)

    return payload


def send_order(token: str, payload: Dict[str, Any], timeout: int = 60) -> requests.Response:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    resp = requests.post(FEED_ORDER_URL, headers=headers, json=payload, timeout=timeout)
    return resp


def iter_orders_from_json(json_path: str) -> List[Dict[str, Any]]:
    with open(json_path, "r") as f:
        data = json.load(f)
    # output_order.json could be a list (multi orders) or a single object
    if isinstance(data, list):
        return data
    return [data]


def main() -> None:
    parser = argparse.ArgumentParser(description="Send orders from output_order.json to feed-order API.")
    parser.add_argument("--json-path", default="output_order.json", help="Path to JSON file (default: output_order.json)")
    parser.add_argument("--dry-run", action="store_true", help="Print payloads without sending")
    parser.add_argument("--only-ref", default=None, help="Send only one outbound_reference")
    parser.add_argument("--username", default="integration_sql_x_armos_system", help="Auth username")
    parser.add_argument("--password", default="QW5kYWkga3UgdGFodSBLYXBhbiB0aWJhIGFqYWxrdSBLdSBha2FuIG1lbW9ob24gVHVoYW4sIHRvbG9uZyBwYW5qYW5na2FuIHVtdXJrdSBBbmRhaSBrdSB0YWh1IChrdSB0YWh1KSBLYXBhbiB0aWJhIG1hc2FrdQ==", help="Auth password")
    args = parser.parse_args()

    orders = iter_orders_from_json(args.json_path)
    if args.only_ref:
        orders = [o for o in orders if str(o.get("outbound_reference")) == str(args.only_ref)]
        if not orders:
            print(f"No order found with outbound_reference={args.only_ref}")
            return

    token = ""
    if not args.dry_run:
        print("Logging in to obtain token...")
        token = login_get_token(args.username, args.password)
        print("Login success. Token acquired.")

    success_count = 0
    fail_count = 0

    for idx, order in enumerate(orders, start=1):
        payload = build_payload(order)
        ref = payload.get("outbound_reference")
        if args.dry_run:
            print(f"\n[DRY-RUN] Order {idx}/{len(orders)} ref={ref}")
            print(json.dumps(payload, indent=2))
            success_count += 1
            continue

        try:
            resp = send_order(token, payload)
            ct = resp.headers.get("content-type", "")
            body = resp.json() if "application/json" in ct else resp.text
            if resp.ok:
                print(f"[OK] {ref}: {resp.status_code} -> {body}")
                success_count += 1
            else:
                print(f"[FAIL] {ref}: {resp.status_code} -> {body}")
                fail_count += 1
        except Exception as e:
            print(f"[ERROR] {ref}: {e}")
            fail_count += 1

    print(f"\nDone. Success: {success_count}, Failed: {fail_count}")


if __name__ == "__main__":
    main()


