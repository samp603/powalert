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

# ======================================================
# Dropbox Auth (supports refresh token + fallback)
# ======================================================
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
DROPBOX_ACCESS_TOKEN = os.getenv("DROPBOX_TOKEN")  # legacy fallback

dbx = None
if DROPBOX_REFRESH_TOKEN and DROPBOX_APP_KEY and DROPBOX_APP_SECRET:
    # Preferred: refreshable token
    dbx = dropbox.Dropbox(
        oauth2_refresh_token=DROPBOX_REFRESH_TOKEN,
        app_key=DROPBOX_APP_KEY,
        app_secret=DROPBOX_APP_SECRET,
    )
    print("✅ Dropbox via refresh token")
elif DROPBOX_ACCESS_TOKEN:
    # Legacy: short-lived access token
    dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
    print("⚠ Dropbox via legacy access token (WILL EXPIRE)")
else:
    print("⚠ No Dropbox credentials found — uploads disabled")


# ======================================================
# Weather logic (Open-Meteo)
# ======================================================
def get_snow_forecast(lat, lon):
    """Return (3h_snow_in, 6h_snow_in)."""
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


# ======================================================
# Cropping helper
# ======================================================
def apply_crop(img_bytes, crop):
    """Return cropped JPEG bytes."""
    if not crop:
        return img_bytes

    x1, y1, x2, y2 = crop
    try:
        img = Image.open(BytesIO(img_bytes))
        cropped = img.crop((x1, y1, x2, y2))
        buf = BytesIO()
        cropped.save(buf, format="JPEG")
        return buf.getvalue()
    except:
        return img_bytes


# ======================================================
# Dropbox last image reference
# ======================================================
def get_latest_dropbox_image(name):
    """Returns bytes of most recent Dropbox image for this mountain, else None."""
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


# ======================================================
# pHash + diff
# ======================================================
def get_phash(image_bytes):
    try:
        img = Image.open(BytesIO(image_bytes))
        return imagehash.phash(img)
    except:
        return None


def is_meaningfully_different(new_bytes, name, crop):
    """
    Compare against newest Dropbox image,
    cropping both images before hashing.
    """
    old_bytes = get_latest_dropbox_image(name)
    if old_bytes is None:
        return True   # no reference

    new_bytes = apply_crop(new_bytes, crop)
    old_bytes = apply_crop(old_bytes, crop)

    old_hash = get_phash(old_bytes)
    new_hash = get_phash(new_bytes)

    if not old_hash or not new_hash:
        return True   # can't compare → keep

    diff = old_hash - new_hash
    return diff > 3       # threshold


# ======================================================
# Dropbox Upload
# ======================================================
def upload_dropbox(local_path, name):
    if dbx is None:
        print("⚠ No Dropbox configured, skipping upload.")
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


# ======================================================
# Main
# ======================================================
def main():
    with open(CFG) as f:
        sources = json.load(f)

    for src in sources:
        name = src["name"]
        url = src["snapshot_url"]
        lat = src.get("lat")
        lon = src.get("lon")
        crop = src.get("crop")

        capture = False

        # Weather gate
        if lat and lon:
            next3, next6 = get_snow_forecast(lat, lon)
            print(name, "→ next3:", next3, "in   next6:", next6, "in")

            if next3 >= 0.25 or next6 >= 1.0:
                capture = True

        # Always fetch snapshot
        try:
            r = requests.get(url, timeout=10, verify=False)
            img_bytes = r.content
        except:
            print(f"⚠ {name}: fetch failed")
            continue

        # Visual-diff gate
        if not capture:
            if not is_meaningfully_different(img_bytes, name, crop):
                print(f"⏭ {name}: skipped (visually same)")
                continue

        # Save locally
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
