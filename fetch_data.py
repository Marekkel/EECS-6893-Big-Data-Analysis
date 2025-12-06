import requests
import json
import time
import datetime
import os

API_KEY = "hU8nqVdaW6fiwWs5AChzZRYxNVgasUtC"  
URL = "https://app.ticketmaster.com/discovery/v2/events.json"

COUNTRY = "US"

# DMA IDs from Ticketmaster docs
DMA_LIST = {
    "NewYork": 345,
    "LosAngeles": 324,
    "Chicago": 249,
    "Houston": 300,
    "Phoenix": 359,
    "SanFrancisco": 347,
    "Miami": 334,      
    "Atlanta": 307,
    "Boston": 306,
    "Dallas": 251,
    "WashingtonDC": 341,
    "Seattle": 350,
    "Denver": 328,
    "Orlando": 336,
    "LasVegas": 323,
    "Philadelphia": 338,
    "Minneapolis": 323,
    "Detroit": 232,
    "SanDiego": 348,
    "AllUS": 200,  
}

OUTPUT_DIR = "ticketmaster_raw/"


def fetch_one_dma(dma_name, dma_id):
    """Fetch all music events for a given DMA (Designated Market Area)."""

    print(f"[INFO] Fetching dma={dma_name} (id={dma_id})")

    page = 0
    saved_records = 0

    # 按当天日期分目录
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    out_path = os.path.join(OUTPUT_DIR, f"dt={today}")
    os.makedirs(out_path, exist_ok=True)

    outfile = os.path.join(out_path, f"events_{dma_name}.jsonl")
    fout = open(outfile, "a", encoding="utf-8")

    while True:
        params = {
            "apikey": API_KEY,
            "classificationName": "music",
            "countryCode": COUNTRY,
            "dmaId": dma_id,
            "page": page,
            "size": 200,   # 单页最多 200
        }

        try:
            resp = requests.get(URL, params=params, timeout=10)
            print(f"[DEBUG] dma={dma_name}, page={page}, status={resp.status_code}")
            data = resp.json()
        except Exception as e:
            print("[ERROR] request/json failed:", e)
            time.sleep(1)
            continue

        # 看看是不是 error
        if "fault" in data or "errors" in data:
            print("[ERROR] API returned error:")
            print(json.dumps(data, indent=2)[:500])
            break

        events = data.get("_embedded", {}).get("events", [])

        if len(events) == 0:
            print(f"[DONE] dma={dma_name}: no more events (page={page}), events=0")
            print("[DEBUG] data keys:", list(data.keys()))
            break

        for e in events:
            fout.write(json.dumps(e) + "\n")
            saved_records += 1

        print(f"[INFO] dma={dma_name}, page={page}, this_page={len(events)}, total_saved={saved_records}")

        page += 1
        time.sleep(0.2)   # 防止速率过高被限流

    fout.close()
    print(f"[INFO] Finished dma={dma_name}, total_saved={saved_records}, file={outfile}")


def main():
    for name, dma_id in DMA_LIST.items():
        fetch_one_dma(name, dma_id)
        time.sleep(1)

    print("[ALL DONE] All DMAs crawled!")


if __name__ == "__main__":
    main()
