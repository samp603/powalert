# stake/collect.py
import os
import json
import requests
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

ROOT = os.path.dirname(os.path.dirname(__file__))
CFG = os.path.join(ROOT, "config", "epic_resorts.json")
LOCAL_DIR = os.path.join(ROOT, "data", "stake_snapshots")

os.makedirs(LOCAL_DIR, exist_ok=True)

# === Load Drive creds ===
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")
GDRIVE_KEY = os.getenv("GDRIVE_KEY")

if not GDRIVE_FOLDER_ID or not GDRIVE_KEY:
    raise RuntimeError("❌ Missing GDRIVE_FOLDER_ID or GDRIVE_KEY secrets")

creds = Credentials.from_service_account_info(
    json.loads(GDRIVE_KEY),
    scopes=["https://www.googleapis.com/auth/drive"]
)

drive = build("drive", "v3", credentials=creds)


# === Drive Helpers ===
def get_or_create_folder(parent_id: str, name: str) -> str:
    """Return folder id under parent, create if missing."""
    query = (
        f"mimeType='application/vnd.google-apps.folder' "
        f"and trashed=false and name='{name}' and '{parent_id}' in parents"
    )
    res = drive.files().list(q=query, fields="files(id)").execute()
    files = res.get("files", [])

    if files:
        return files[0]["id"]

    # create
    file_metadata = {
        "name": name,
        "parents": [parent_id],
        "mimeType": "application/vnd.google-apps.folder",
    }
    folder = drive.files().create(body=file_metadata, fields="id").execute()
    return folder["id"]


def upload_to_drive(local_path: str, resort: str):
    """Upload snapshot to /Resort/YYYY-MM-DD/filename"""
    # folder C structure
    date_folder = datetime.now().strftime("%Y-%m-%d")
    resort_folder_id = get_or_create_folder(GDRIVE_FOLDER_ID, resort)
    date_folder_id = get_or_create_folder(resort_folder_id, date_folder)

    fname = os.path.basename(local_path)
    media = MediaFileUpload(local_path, mimetype="image/jpeg")

    file_metadata = {
        "name": fname,
        "parents": [date_folder_id],
    }

    drive.files().create(body=file_metadata, media_body=media).execute()
    print(f"📁 Uploaded → Drive: {resort}/{date_folder}/{fname}")


def fetch_and_save(resort: dict):
    name = resort["name"]
    stake = resort.get("stake")
    if not stake:
        print(f"⚠ No stake config for {name}")
        return

    url = stake.get("snapshot_url") or resort.get("url")
    if not url:
        print(f"⚠ No snapshot URL for {name}")
        return

    # save local file
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    fname = f"{name}_{timestamp}.jpg"
    local_path = os.path.join(LOCAL_DIR, fname)

    try:
        # Allow bad SSL so streams don’t break
        r = requests.get(url, timeout=12, verify=False)
        if r.status_code != 200:
            print(f"⚠ {name}: HTTP {r.status_code}")
            return
    except Exception as e:
        print(f"⚠ {name}: fetch failed -> {e}")
        return

    with open(local_path, "wb") as f:
        f.write(r.content)

    print(f"✅ Saved: {local_path}")

    # upload to drive
    try:
        upload_to_drive(local_path, name)
    except Exception as e:
        print(f"❌ Upload failed for {name}: {e}")


def main():
    with open(CFG) as f:
        resorts = json.load(f)

    for r in resorts:
        fetch_and_save(r)


if __name__ == "__main__":
    main()
