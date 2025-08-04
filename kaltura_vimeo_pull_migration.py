import os
import requests
import json
import xml.etree.ElementTree as ET
import time

# --- Configuration ---
# Your Kaltura Credentials
# Replace these with your actual Kaltura partner ID and secret.
KALTURA_PARTNER_ID = "YOUR_KALTURA_PARTNER_ID"
KALTURA_USER_SECRET = "YOUR_KALTURA_USER_SECRET"
KALTURA_SERVICE_URL = "https://www.kaltura.com/api_v3/index.php"

# Your Vimeo Credentials and Destination Folder
# Replace these with your Vimeo access token and the ID of the folder you want to upload to.
VIMEO_ACCESS_TOKEN = "YOUR_VIMEO_ACCESS_TOKEN"
VIMEO_FOLDER_ID = "YOUR_VIMEO_FOLDER_ID"

# List of entry IDs to migrate
# Replace these with the actual Kaltura entry IDs you want to process.
KALTURA_ENTRY_IDS = [
    "YOUR_KALTURA_ENTRY_ID_1",
    "YOUR_KALTURA_ENTRY_ID_2",
    "YOUR_KALTURA_ENTRY_ID_3",
    # Add all other entry IDs here
]


# --- Kaltura API Functions ---
def get_kaltura_session():
    """Generates a new session (KS) token for a short-lived session."""
    params = {
        "service": "session",
        "action": "start",
        "format": 1,
        "secret": KALTURA_USER_SECRET,
        "partnerId": KALTURA_PARTNER_ID,
        "type": 0
    }
    try:
        response = requests.post(KALTURA_SERVICE_URL, data=params)
        response.raise_for_status()

        # Handle the quirky Kaltura session response: it can be a raw string or a JSON object.
        response_data = response.json()
        
        if isinstance(response_data, dict):
            return response_data.get('ks')
        elif isinstance(response_data, str):
            return response_data
        else:
            print("Kaltura API response was of an unexpected type. Cannot get KS token.")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error getting Kaltura session: {e}")
        return None

def get_video_metadata_and_direct_url(ks_token, entry_id):
    """
    Retrieves video metadata (title) and constructs a direct,
    authenticated download URL for the highest quality available MP4 flavor.
    """
    # Step 1: Get media metadata
    media_params = {
        "service": "media",
        "action": "get",
        "ks": ks_token,
        "partnerId": KALTURA_PARTNER_ID,
        "entryId": entry_id,
        "format": 1
    }
    try:
        media_response = requests.get(KALTURA_SERVICE_URL, params=media_params)
        media_response.raise_for_status()
        media_data = media_response.json()
        title = media_data.get('name')
    except (requests.exceptions.RequestException, json.JSONDecodeError, AttributeError) as e:
        print(f"Error retrieving metadata for entry {entry_id}: {e}")
        return None, None, None

    # Step 2: Get flavor assets to find the best MP4 ID
    flavor_params = {
        "service": "flavorasset",
        "action": "getByEntryId",
        "format": 1,
        "entryId": entry_id,
        "ks": ks_token
    }
    try:
        flavor_response = requests.post(KALTURA_SERVICE_URL, data=flavor_params)
        flavor_response.raise_for_status()

        # Check if the response is a list (JSON) or an error string
        flavor_assets = flavor_response.json()
        if not isinstance(flavor_assets, list):
            print(f"  -> API returned an error for flavor assets on entry {entry_id}.")
            print(f"  -> Raw API response: {flavor_response.text}")
            return title, None, None
        
        # Filter for ready MP4 flavors
        mp4_flavors = [asset for asset in flavor_assets if asset.get('fileExt') == 'mp4' and asset.get('status') == 2]

        if not mp4_flavors:
            print(f"  -> No valid MP4 flavor found for entry {entry_id}. Skipping.")
            return title, None, None

        # Sort flavors by resolution (e.g., width) to get the highest quality
        mp4_flavors.sort(key=lambda x: int(x.get('width', 0)), reverse=True)
        
        # The highest quality flavor is now the first in the list
        best_flavor = mp4_flavors[0]
        flavor_id = best_flavor.get('id')

        if flavor_id:
            # Construct the direct, authenticated download URL
            download_url = f"https://www.kaltura.com/p/{KALTURA_PARTNER_ID}/sp/{KALTURA_PARTNER_ID}00/playManifest/entryId/{entry_id}/flavorId/{flavor_id}/format/download/protocol/https?ks={ks_token}"
            return title, None, download_url # Removed tags for simplicity, as per user's request.
        
        print(f"  -> No valid flavor ID found for entry {entry_id}. Skipping.")
        return title, None, None
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        print(f"Error getting flavor assets for entry {entry_id}: {e}")
        return title, None, None


# --- Vimeo API Function ---
def initiate_vimeo_pull_upload(download_url, title):
    """
    Initiates a 'pull' upload to Vimeo from a given URL and then moves it
    to a specified folder.
    """
    headers = {
        "Authorization": f"Bearer {VIMEO_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/vnd.vimeo.*+json;version=3.4"
    }

    # Step 1: Initiate the pull upload
    upload_data = {
        "upload": {
            "approach": "pull",
            "link": download_url
        },
        "name": title,
    }

    try:
        print(f"  -> Starting pull upload for '{title}'...")
        post_response = requests.post("https://api.vimeo.com/me/videos", headers=headers, json=upload_data)
        post_response.raise_for_status()
        
        vimeo_video_data = post_response.json()
        new_video_uri = vimeo_video_data.get('uri')
        new_video_id = new_video_uri.split('/')[-1]

        print(f"  -> Successfully initiated pull upload. Video URI: {new_video_uri}")

    except requests.exceptions.RequestException as e:
        print(f"  -> Failed to start Vimeo pull upload for '{title}': {e}")
        return

    # Step 2: Move the video to the specified folder
    try:
        print(f"  -> Moving video {new_video_id} to folder {VIMEO_FOLDER_ID}...")
        move_url = f"https://api.vimeo.com/me/projects/{VIMEO_FOLDER_ID}/videos/{new_video_id}"
        put_response = requests.put(move_url, headers=headers)
        put_response.raise_for_status()

        print(f"  -> Success: Video '{title}' moved to folder {VIMEO_FOLDER_ID}.")

    except requests.exceptions.RequestException as e:
        print(f"  -> Failed to move video '{title}' to folder {VIMEO_FOLDER_ID}: {e}")

# --- Main Migration Logic ---
if __name__ == "__main__":
    print("Starting Kaltura to Vimeo migration (Pull method)...")

    # Get a Kaltura session token (KS)
    ks_token = get_kaltura_session()
    if not ks_token:
        print("Failed to get Kaltura session token. Exiting.")
        exit(1)
    
    print("Successfully obtained Kaltura Session Token.")

    # Iterate through the list of Kaltura entries
    for entry_id in KALTURA_ENTRY_IDS:
        print(f"\nProcessing Kaltura entry ID: {entry_id}...")
        title, _, download_url = get_video_metadata_and_direct_url(ks_token, entry_id)
        
        # Only proceed if we have a valid title and download URL
        if not title or not download_url:
            print(f"Skipping entry {entry_id} due to missing data.")
            continue
        
        # Initiate the upload to Vimeo
        initiate_vimeo_pull_upload(download_url, title)

    print("\nMigration script completed.")
