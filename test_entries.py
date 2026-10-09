"""Create / edit / delete ONE test invoice in Bluepeak through the QBO API.

python test_entries.py create "Your Full Name"
python test_entries.py edit
python test_entries.py delete

Safety: edit and delete only touch the invoice id saved by 'create' in
test_state.json, and refuse to act unless its memo starts with 'TEST'.
Uses the same environment variables and token handling as qb_sync.py.
"""
import json, os, sys
import requests
import qb_sync as q          # reuses auth (refresh token kept in BigQuery) and base URL

class ApiError(Exception):
    pass


STATE = "test_state.json"
DASH = "\u2013"              # the dash in 'TEST – Your Full Name'


def call(method, path, params=None, body=None):
    if q._access is None:
        q.refresh_access_token()
    for attempt in range(2):
        r = requests.request(method, f"{q.BASE}/company/{q.REALM}/{path}",
                             params={"minorversion": "75", **(params or {})}, json=body, timeout=60,
                             headers={"Authorization": f"Bearer {q._access}", "Accept": "application/json"})
        if r.status_code == 401 and attempt == 0:
            q.refresh_access_token(); continue
        if r.status_code != 200:
            raise ApiError(f"API error {r.status_code}: {r.text}")
        return r.json()


def load_state():
    if not os.path.exists(STATE):
        sys.exit("No test_state.json. Run 'create' first.")
    return json.load(open(STATE))


def get_test_invoice(inv_id):
    inv = call("GET", f"invoice/{inv_id}")["Invoice"]
    memo = inv.get("PrivateNote", "")
    if not memo.startswith("TEST"):
        sys.exit("Safety stop: this invoice's memo does not start with TEST. Not touching it.")
    return inv


def create(name):
    memo = f"TEST {DASH} {name}"
    cust = call("POST", "customer", body={"DisplayName": memo})["Customer"]
    items = call("GET", "query", {"query": "select * from Item where Type = 'Service' MAXRESULTS 1"})
    item = items["QueryResponse"]["Item"][0]            # read-only: we just reference an existing item
    inv = call("POST", "invoice", body={
        "CustomerRef": {"value": cust["Id"]},
        "TxnDate": "2026-09-01", "DueDate": "2026-09-15",
        "PrivateNote": memo, "CustomerMemo": {"value": memo},
        "Line": [{"DetailType": "SalesItemLineDetail", "Amount": 100,
                  "SalesItemLineDetail": {"ItemRef": {"value": item["Id"]}, "Qty": 1, "UnitPrice": 100}}],
    })["Invoice"]
    json.dump({"customer_id": cust["Id"], "invoice_id": inv["Id"]}, open(STATE, "w"))
    print(f"Created customer {cust['Id']} and invoice {inv['Id']} (total {inv['TotalAmt']}).")


def edit():
    st = load_state()
    inv = get_test_invoice(st["invoice_id"])
    base = {"Id": inv["Id"], "SyncToken": inv["SyncToken"], "sparse": True}
    # Attempt 1: change the line amount, sending only the fields we need (no copied account refs).
    lines = []
    for l in inv["Line"]:
        if l["DetailType"] == "SalesItemLineDetail":
            d = l["SalesItemLineDetail"]
            lines.append({"Id": l["Id"], "DetailType": "SalesItemLineDetail", "Amount": 150,
                          "SalesItemLineDetail": {"ItemRef": d["ItemRef"], "Qty": 1, "UnitPrice": 150}})
    try:
        out = call("POST", "invoice", body={**base, "Line": lines})["Invoice"]
        print(f"Edited invoice {out['Id']}: total now {out['TotalAmt']}.")
        return
    except ApiError as e:
        print("Amount edit was rejected by QuickBooks, editing the memo instead.")
        print(str(e)[:300].encode("ascii", "replace").decode())
    # Attempt 2: edit the memo only (still starts with TEST).
    memo = inv["PrivateNote"] + " (edited)"
    out = call("POST", "invoice", body={**base, "PrivateNote": memo,
                                        "CustomerMemo": {"value": memo}})["Invoice"]
    print(f"Edited memo of invoice {out['Id']}.")


def delete():
    st = load_state()
    inv = get_test_invoice(st["invoice_id"])
    call("POST", "invoice", params={"operation": "delete"},
         body={"Id": inv["Id"], "SyncToken": inv["SyncToken"]})
    print(f"Deleted invoice {inv['Id']}.")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    try:
        if cmd == "create" and len(sys.argv) > 2:
            create(" ".join(sys.argv[2:]))
        elif cmd == "edit":
            edit()
        elif cmd == "delete":
            delete()
        else:
            sys.exit(__doc__)
    except ApiError as e:
        sys.exit(str(e))
