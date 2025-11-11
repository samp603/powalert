# stake/collect.py
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

    # snowfall is in cm → convert to inches
    def cm_to_in(x):
        return (x or 0) / 2.54

    next3 = sum(cm_to_in(x) for x in snowfall[:3])
    next6 = sum(cm_to_in(x) for x in snowfall[:6])

    return (next3, next6)


# -------------------------------
# Perceptual difference check
# -------------------------------
def get_phash(image_bytes):
    try:
        img = Image.open(BytesIO(image_bytes))
        return imagehash.phash(img)
    except:
        return None


def is_new_image(name, new_bytes):
    """
    Compare against most recent local copy via pHash.
    If very similar, skip.
    """
    folder = os.path.join(OUT, name)
    if not os.path.exists(folder):
        return True

    files = sorted([f for f in os.listdir(folder) if f.endswith(".jpg")])
    if not files:
        return True

    last_path = os.path.join(folder, files[-1])
    with open(last_path, "rb") as f:
        old_bytes = f.read()

    old_hash = get_phash(old_bytes)
    new_hash = get_phash(new_bytes)

    if not old_hash or not new_hash:
        return True

    # hamming distance
    diff = old_hash - new_hash

    # If images differ enough → keep
    return diff > 3   # small difference = likely identical


# -------------------------------
# Dropbox Upload
# -------------------------------
def upload_dropbox(local_path, folder_name):
    if dbx is None:
        print("⚠ No Dropbox token configured, skipping upload.")
        return

    remote_folder = f"/powalert/{folder_name}"
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
        url = src["snapshot_url"]
        lat = src.get("lat")
        lon = src.get("lon")

        # ---- Weather gate ----
        capture = False

        if lat and lon:
            next3, next6 = get_snow_forecast(lat, lon)
            print(name, "→ next3:", next3, "in   next6:", next6, "in")

            if next3 >= 0.25 or next6 >= 1.0:
                capture = True

        # ---- Pull snapshot regardless, so we can compute pHash ----
        try:
            r = requests.get(url, timeout=10, verify=False)
            img_bytes = r.content
        except:
            print(f"⚠ {name}: fetch failed")
            continue

        # pHash rule (store if meaningfully different)
        if is_new_image(name, img_bytes):
            capture = True

        # Always capture if folder doesn’t exist yet
        if not os.path.exists(os.path.join(OUT, name)):
            capture = True

        if not capture:
            print(f"⏭ {name}: skipped (no snow + looks same)")
            continue

        # Save image
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
