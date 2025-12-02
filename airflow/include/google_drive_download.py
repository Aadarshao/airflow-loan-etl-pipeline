"""
Helper functions for interacting with Google Drive using a service account.

Exposes two main functions:

- list_files_in_folder(folder_id)
- download_file_from_drive(file_id, dest_path)

This module expects a service account JSON file at:
    airflow/include/drive_service_account.json

Inside the container, that path is:
    /opt/airflow/include/drive_service_account.json

Make sure your service account has access to the shared Google Drive folder
(either directly or via domain-wide delegation + impersonation if required).
"""

import io
import os
from typing import List, Dict, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# Scope with read-only access to Drive files
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def _get_service_account_path() -> str:
    """
    Resolve the service account JSON path.

    We assume the JSON file is located alongside this module:
        airflow/include/drive_service_account.json
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "drive_service_account.json")


def _get_drive_service():
    """
    Build and return an authenticated Google Drive API service
    using a service account.

    We explicitly disable the discovery file cache to avoid the
    'file_cache is only supported with oauth2client<4.0.0' warning.
    """
    sa_path = _get_service_account_path()
    if not os.path.exists(sa_path):
        raise FileNotFoundError(
            f"Google Drive service account file not found at: {sa_path}"
        )

    credentials = service_account.Credentials.from_service_account_file(
        sa_path, scopes=SCOPES
    )
    # If using domain-wide delegation, you can do:
    # delegated_creds = credentials.with_subject("user@yourdomain.com")
    # return build("drive", "v3", credentials=delegated_creds, cache_discovery=False)

    service = build("drive", "v3", credentials=credentials, cache_discovery=False)
    return service


def list_files_in_folder(
    folder_id: str,
    name_prefix: str = "loan_",
    mime_type: str = "text/csv",
    page_size: int = 100,
) -> List[Dict[str, Any]]:
    """
    List files in a given Google Drive folder, filtered by name prefix and mime type.

    Args:
        folder_id: ID of the folder in Google Drive.
        name_prefix: Only return files whose name starts with this prefix.
        mime_type: Only return files of this MIME type.
        page_size: Maximum number of files per page (API pagination).

    Returns:
        A list of file metadata dictionaries containing:
            id, name, mimeType, modifiedTime, size, webViewLink
    """
    service = _get_drive_service()

    # Building the query
    # Example: "'<folder_id>' in parents and name contains 'loan_' and mimeType='text/csv' and trashed=false"
    q_parts = [
        f"'{folder_id}' in parents",
        "trashed = false",
    ]

    if name_prefix:
        # Use 'name contains' for simplicity; DAG can further filter
        q_parts.append(f"name contains '{name_prefix}'")

    if mime_type:
        q_parts.append(f"mimeType = '{mime_type}'")

    query = " and ".join(q_parts)

    results: List[Dict[str, Any]] = []
    page_token = None

    while True:
        response = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType, modifiedTime, size, webViewLink)",
                pageSize=page_size,
                pageToken=page_token,
            )
            .execute()
        )

        files = response.get("files", [])
        results.extend(files)

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return results


def download_file_from_drive(file_id: str, dest_path: str) -> None:
    """
    Download a Google Drive file by ID to a local path.

    Args:
        file_id: The ID of the file in Google Drive.
        dest_path: Local filesystem path where the file will be written.
    """
    service = _get_drive_service()

    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    # Ensure destination directory exists
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    with open(dest_path, "wb") as f:
        f.write(fh.getvalue())
