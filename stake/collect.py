import os
import json
import requests
from datetime import datetime, UTC

ROOT = os.path.dirname(os.path.dirname(__file__))
CFG = os.path.join(ROOT, "stake", "stake_sources.json")
OUT_DIR = os.path.join(ROOT, "data", "stake_snapshots")
os.makedirs(OUT_DIR, exist_ok=True)


def fetch_snapshot(name, url):
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            print(f"⚠ {name}: HTTP {r.status_code}")
            return None
    except Exception as e:
        print(f"⚠ {name}: fetch failed -> {e}")
        return None

    fname = f"{name}_{datetime.now(UTC).strftime('%Y%m%d%H%M')}.jpg"
    path = os.path.join(OUT_DIR, fname)

    with open(path, "wb") as f:
        f.write(r.content)

    print(f"✅ Saved: {path}")
    return path


def collect_all():
    with open(CFG) as f:
        sources = json.load(f)

    for src in sources:
        name = src["name"].replace(" ", "")
        url = src["snapshot_url"]
        fetch_snapshot(name, url)


if __name__ == "__main__":
    collect_all()
