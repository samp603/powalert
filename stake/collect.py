import os
import requests
import dropbox
from datetime import datetime, UTC
import json
from PIL import Image
import imagehash
from io import BytesIO

ROOT = os.path.dirname(os.path.dirname(__file__))
CFG = os.path.join(ROOT, "stake", "stake_sources.json")
OUT = os.path.join(ROOT, "data", "stake_snapshots")

os.makedirs(OUT, exist_ok=True)

DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")
dbx = dropbox.Dropbox(DROPBOX_TOKEN) if DROPBOX_TOKEN else None

# -------------------------------
# Weather logic (Open-Meteo)
# -------------------------------
def get_snow_forecast(lat, lon):
    """Return (3h_snow_in, 6h_snow_in)"""
    url = (
        "https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}"
        "&hourly=snowfall&timezone=UTC"
    )

    try:
        r = requests.get(url, timeout=10)
        j = r.json()
    except:
        return (0, 0)

    hourly = j.get("hourly", {})
    snowfall = hourly.get("snowfall", [])
    times = hourly.get("time", [])

    if not snowfall or not times:
        return (0, 0)

    def cm_to_in(x):
        return (x or 0) / 2.54

    next3 = sum(cm_to_in(x) for x in snowfall[:3])
    next6 = sum(cm_to_in(x) for x in snowfall[:6])

    return (next3, next6)


# -------------------------------
# Dropbox last image pHash reference
# -------------------------------
def get_latest_dropbox_image(name):
    """Returns bytes of most recent Dropbox image for this mountain, else None"""
    if not dbx:
        return None

    folder = f"/powalert/{name}"

    try:
        res = dbx.files_list_folder(folder)
    except:
        return None

    files = [
        entry for entry in res.entries
        if isinstance(entry, dropbox.files.FileMetadata)
    ]

    if not files:
        return None

    files.sort(key=lambda f: f.client_modified, reverse=True)
    latest = files[0]

    try:
        metadata, response = dbx.files_download(latest.path_lower)
        return response.content
    except:
        return None


# -------------------------------
# pHash + diff
# -------------------------------
def get_phash(image_bytes):
    try:
        img = Image.open(BytesIO(image_bytes))
        return imagehash.phash(img)
    except:
        return None


def is_meaningfully_different(new_bytes, name):
    """Compare against newest Dropbox image."""
    old_bytes = get_latest_dropbox_image(name)
    if old_bytes is None:
        return True  # nothing to compare against — keep it

    old_hash = get_phash(old_bytes)
    new_hash = get_phash(new_bytes)

    if not old_hash or not new_hash:
        return True  # can’t compare — keep just in case

    diff = old_hash - new_hash
    # Higher threshold lowers sensitivity; 3–5 is reasonable
    return diff > 3


# -------------------------------
# Dropbox Upload
# -------------------------------
def upload_dropbox(local_path, name):
    if dbx is None:
        print("⚠ No Dropbox token configured, skipping upload.")
        return

    remote_folder = f"/powalert/{name}"
    remote_file = f"{remote_folder}/{os.path.basename(local_path)}"

    try:
        with open(local_path, "rb") as f:
            dbx.files_upload(
                f.read(),
                remote_file,
                mode=dropbox.files.WriteMode.overwrite
            )
        print(f"✅ Uploaded → {remote_file}")
    except Exception as e:
        print(f"❌ Upload failed → {e}")


# -------------------------------
# Main
# -------------------------------
def main():
    with open(CFG) as f:
        sources = json.load(f)

    for src in sources:
        name = src["name"]
        url  = src["snapshot_url"]
        lat  = src.get("lat")
        lon  = src.get("lon")

        capture = False

        # Weather check
        if lat and lon:
            next3, next6 = get_snow_forecast(lat, lon)
            print(name, "→ next3:", next3, "in   next6:", next6, "in")

            # meaningfully snowy?
            if next3 >= 0.25 or next6 >= 1.0:
                capture = True

        # always download raw
        try:
            r = requests.get(url, timeout=10, verify=False)
            img_bytes = r.content
        except:
            print(f"⚠ {name}: fetch failed")
            continue

        # pHash decide
        if is_meaningfully_different(img_bytes, name):
            capture = True
        else:
            print(f"⏭ {name}: skipped (visually same)")
            continue

        # Store locally
        folder = os.path.join(OUT, name)
        os.makedirs(folder, exist_ok=True)

        fname = f"{name}_{datetime.now(UTC).strftime('%Y%m%d%H%M')}.jpg"
        local_path = os.path.join(folder, fname)

        with open(local_path, "wb") as f:
            f.write(img_bytes)

        print(f"✅ Saved: {local_path}")

        upload_dropbox(local_path, name)


if __name__ == "__main__":
    main()
