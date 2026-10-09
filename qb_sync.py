"""QuickBooks Online (sandbox) -> BigQuery incremental sync (append-only, free-tier safe).

python qb_sync.py                 # incremental run (what the scheduler runs)
python qb_sync.py --full          # force full reload of all entities
python qb_sync.py --reconcile     # only compare QBO counts vs BigQuery

Design: BigQuery's free sandbox blocks DML (MERGE/UPDATE/INSERT), so we only use
load jobs to APPEND rows to <entity>_history. A view <entity> shows the latest
version of each record (deleted ones are flagged is_deleted = TRUE).
"""
import argparse, datetime as dt, json, logging, os, sys, time
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

ENTITIES = ["Account", "Customer", "Vendor", "Invoice", "Payment",
            "Bill", "BillPayment", "JournalEntry"]
HAS_ACTIVE = {"Account", "Customer", "Vendor"}   # inactive records hidden by default
BASE = os.environ.get("QBO_BASE_URL", "https://sandbox-quickbooks.api.intuit.com/v3")
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
PAGE = 1000
CDC_MAX_AGE = dt.timedelta(days=25)   # CDC only looks back 30 days
OVERLAP = dt.timedelta(minutes=5)     # re-read a small window; unchanged rows are skipped

log = logging.getLogger("qb")
REALM = os.environ["QBO_REALM_ID"]
DATASET = os.environ.get("BQ_DATASET", "quickbooks")
bq = bigquery.Client(project=os.environ.get("BQ_PROJECT") or None,
                     location=os.environ.get("BQ_LOCATION", "US"))
PROJECT = bq.project
T = lambda name: f"`{PROJECT}.{DATASET}.{name}`"
ROW_SCHEMA = [bigquery.SchemaField("id", "STRING"), bigquery.SchemaField("sync_token", "STRING"),
              bigquery.SchemaField("last_updated", "TIMESTAMP"), bigquery.SchemaField("is_deleted", "BOOL"),
              bigquery.SchemaField("synced_at", "TIMESTAMP"), bigquery.SchemaField("payload", "STRING")]
STATE_SCHEMA = [bigquery.SchemaField("k", "STRING"), bigquery.SchemaField("v", "STRING"),
                bigquery.SchemaField("updated_at", "TIMESTAMP")]


def append_rows(table, rows, schema):
    cfg = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema=schema)
    bq.load_table_from_json(rows, f"{PROJECT}.{DATASET}.{table}", job_config=cfg).result()


# ---------- setup ----------
def ensure_objects():
    bq.create_dataset(bigquery.Dataset(f"{PROJECT}.{DATASET}"), exists_ok=True)
    bq.create_table(bigquery.Table(f"{PROJECT}.{DATASET}._state", schema=STATE_SCHEMA), exists_ok=True)
    for e in ENTITIES:
        n = e.lower()
        hist = f"{PROJECT}.{DATASET}.{n}_history"
        bq.create_table(bigquery.Table(hist, schema=ROW_SCHEMA), exists_ok=True)
        ref = f"{PROJECT}.{DATASET}.{n}"
        try:
            obj = bq.get_table(ref)
        except NotFound:
            obj = None
        if obj is not None and obj.table_type == "TABLE":   # empty leftover from the first version
            if obj.num_rows:
                sys.exit(f"{ref} is a non-empty table; delete it manually so a view can take its name.")
            bq.delete_table(ref)
            obj = None
        if obj is None:
            view = bigquery.Table(ref)
            view.view_query = f"""SELECT * EXCEPT(rn) FROM (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY synced_at DESC) rn
              FROM `{hist}`) WHERE rn = 1"""
            bq.create_table(view)


# ---------- state (append-only; latest row per key wins) ----------
def get_state(k):
    rows = list(bq.query(f"SELECT v FROM {T('_state')} WHERE k=@k ORDER BY updated_at DESC LIMIT 1",
        job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("k", "STRING", k)])).result())
    return rows[0].v if rows else None

def set_state(k, v):
    append_rows("_state", [{"k": k, "v": v, "updated_at": dt.datetime.now(dt.timezone.utc).isoformat()}], STATE_SCHEMA)


# ---------- auth ----------
_access = None
def refresh_access_token():
    """Intuit rotates refresh tokens: always persist the newly returned one."""
    global _access
    rt = get_state("refresh_token") or os.environ["QBO_REFRESH_TOKEN"]
    r = requests.post(TOKEN_URL, auth=(os.environ["QBO_CLIENT_ID"], os.environ["QBO_CLIENT_SECRET"]),
                      headers={"Accept": "application/json"},
                      data={"grant_type": "refresh_token", "refresh_token": rt}, timeout=60)
    if r.status_code != 200:
        sys.exit(f"Token refresh failed {r.status_code}: {r.text}")
    j = r.json()
    set_state("refresh_token", j["refresh_token"])   # save before anything else
    _access = j["access_token"]

def api(path, params=None):
    global _access
    if _access is None:
        refresh_access_token()
    p = {"minorversion": "75", **(params or {})}
    for attempt in range(6):
        r = requests.get(f"{BASE}/company/{REALM}/{path}", params=p, timeout=60,
                         headers={"Authorization": f"Bearer {_access}", "Accept": "application/json"})
        if r.status_code == 401:
            refresh_access_token(); continue
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(2 ** attempt); continue
        if r.status_code != 200:
            sys.exit(f"API error {r.status_code} on {path}: {r.text}")
        return r.json()
    sys.exit(f"Gave up on {path}")

def where_active(e):
    return " where Active in (true,false)" if e in HAS_ACTIVE else ""


# ---------- extraction ----------
def fetch_all(e):
    out, pos = [], 1
    while True:
        q = f"select * from {e}{where_active(e)} ORDERBY Id STARTPOSITION {pos} MAXRESULTS {PAGE}"
        batch = api("query", {"query": q}).get("QueryResponse", {}).get(e, [])
        out += batch
        if len(batch) < PAGE:
            return out
        pos += PAGE

def fetch_cdc(e, since):
    """Returns (changed_records, hit_cap). Deleted records carry status == 'Deleted'."""
    ts = since.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    resp = api("cdc", {"entities": e, "changedSince": ts})["CDCResponse"][0]["QueryResponse"]
    recs = [r for q in resp for r in q.get(e, [])]
    return recs, len(recs) >= PAGE

def qbo_count(e):
    return api("query", {"query": f"select count(*) from {e}{where_active(e)}"})["QueryResponse"]["totalCount"]


# ---------- load ----------
def to_row(rec, now):
    lu = (rec.get("MetaData") or {}).get("LastUpdatedTime")
    return {"id": str(rec["Id"]), "sync_token": rec.get("SyncToken"),
            "last_updated": lu, "is_deleted": rec.get("status") == "Deleted",
            "synced_at": now, "payload": json.dumps(rec)}

def current_state(e):
    q = f"SELECT id, sync_token, is_deleted FROM {T(e.lower())}"
    return {r.id: (r.sync_token, r.is_deleted) for r in bq.query(q).result()}

def load(e, records, full):
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    rows = {str(r["Id"]): to_row(r, now) for r in records}   # de-dupe by id
    cur = current_state(e)
    if full:   # anything we hold that QBO no longer returns is gone: write a tombstone
        for i, (_, deleted) in cur.items():
            if i not in rows and not deleted:
                rows[i] = {"id": i, "sync_token": None, "last_updated": None, "is_deleted": True,
                           "synced_at": now, "payload": json.dumps({"Id": i, "status": "Deleted"})}
    def unchanged(i, row):
        c = cur.get(i)
        return c is not None and c[1] == row["is_deleted"] and (row["is_deleted"] or c[0] == row["sync_token"])
    out = [row for i, row in rows.items() if not unchanged(i, row)]
    if out:
        append_rows(f"{e.lower()}_history", out, ROW_SCHEMA)
    return len(out)


# ---------- orchestration ----------
def sync_entity(e, force_full):
    started = dt.datetime.now(dt.timezone.utc)
    wm = get_state(f"watermark:{e}")
    wm = dt.datetime.fromisoformat(wm) if wm else None
    full = force_full or wm is None or started - wm > CDC_MAX_AGE
    if not full:
        recs, capped = fetch_cdc(e, wm - OVERLAP)
        if capped:
            log.warning("%s: CDC page cap hit, falling back to full resync", e)
            full = True
    if full:
        recs = fetch_all(e)
    n = load(e, recs, full)
    set_state(f"watermark:{e}", started.isoformat())   # only after a successful load
    log.info("%s: %s, %d fetched, %d rows written", e, "FULL" if full else "incremental", len(recs), n)

def reconcile():
    ok = True
    for e in ENTITIES:
        mine = list(bq.query(f"SELECT COUNT(*) c FROM {T(e.lower())} WHERE NOT is_deleted").result())[0].c
        theirs = qbo_count(e)
        ok &= mine == theirs
        log.info("%-8s %-12s qbo=%d bigquery=%d", "OK" if mine == theirs else "MISMATCH", e, theirs, mine)
    return ok

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true")
    ap.add_argument("--reconcile", action="store_true")
    a = ap.parse_args()
    ensure_objects()
    if not a.reconcile:
        for e in ENTITIES:
            sync_entity(e, a.full)
    sys.exit(0 if reconcile() else 2)
