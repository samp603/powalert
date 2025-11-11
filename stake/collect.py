import os
import requests
import dropbox
from datetime import datetime, UTC
import json

ROOT = os.path.dirname(os.path.dirname(__file__))
CFG = os.path.join(ROOT, "stake", "stake_sources.json")
OUT = os.path.join(ROOT, "data", "stake_snapshots")

os.makedirs(OUT, exist_ok=True)

DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")
dbx = dropbox.Dropbox(DROPBOX_TOKEN) if DROPBOX_TOKEN else None


def upload_dropbox(local_path, folder_name):
    if dbx is None:
        print("⚠ No Dropbox token configured, skipping upload.")
        return

    # Folder inside Dropbox
    remote_folder = f"/powalert/{folder_name}"
    remote_file = f"{remote_folder}/{os.path.basename(local_path)}"

    try:
        # Ensure folder exists — Dropbox auto-creates on upload
        with open(local_path, "rb") as f:
            dbx.files_upload(
                f.read(),
                remote_file,
                mode=dropbox.files.WriteMode.overwrite
            )
        print(f"✅ Uploaded → {remote_file}")
    except Exception as e:
        print(f"❌ Upload failed → {e}")


def main():
    with open(CFG) as f:
        sources = json.load(f)

    for src in sources:
        name = src["name"]
        url = src["snapshot_url"]

        try:
            r = requests.get(url, timeout=10, verify=False)  # SSL workaround
        except Exception as e:
            print(f"⚠ {name}: fetch failed → {e}")
            continue

        if r.status_code != 200:
            print(f"⚠ {name}: snapshot HTTP {r.status_code}")
            continue

        fname = f"{name}_{datetime.now(UTC).strftime('%Y%m%d%H%M')}.jpg"
        local_path = os.path.join(OUT, fname)

        with open(local_path, "wb") as f:
            f.write(r.content)

        print(f"✅ Saved: {local_path}")

        upload_dropbox(local_path, name)


if __name__ == "__main__":
    main()
