"""Private Cloud Storage operations for admission documents."""

from io import BytesIO
from typing import Protocol

from google.auth.exceptions import GoogleAuthError
from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import storage

from shared.observability.errors import AppException
from starlette import status


class DocumentStorage(Protocol):
    def upload(self, object_name: str, content: bytes, content_type: str) -> None: ...

    def download(self, object_name: str) -> bytes: ...

    def delete(self, object_name: str) -> None: ...


class GoogleCloudDocumentStorage:
    """Uploads and retrieves document bytes without exposing bucket paths."""

    def __init__(self, bucket_name: str, project_id: str | None = None) -> None:
        if not bucket_name:
            raise ValueError("PATIENT_DOCUMENTS_BUCKET must be configured")
        self._client = storage.Client(project=project_id)
        self._bucket = self._client.bucket(bucket_name)

    def upload(self, object_name: str, content: bytes, content_type: str) -> None:
        blob = self._bucket.blob(object_name)
        try:
            blob.upload_from_file(
                BytesIO(content),
                rewind=True,
                content_type=content_type,
                if_generation_match=0,
            )
        except (GoogleAPIError, GoogleAuthError) as exc:
            raise AppException(
                "SERVICE_UNAVAILABLE",
                "Document storage is temporarily unavailable",
                status.HTTP_503_SERVICE_UNAVAILABLE,
                retryable=True,
            ) from exc

    def download(self, object_name: str) -> bytes:
        blob = self._bucket.blob(object_name)
        try:
            return blob.download_as_bytes()
        except NotFound as exc:
            raise AppException(
                "NOT_FOUND",
                "Document object was not found",
                status.HTTP_404_NOT_FOUND,
            ) from exc
        except (GoogleAPIError, GoogleAuthError) as exc:
            raise AppException(
                "SERVICE_UNAVAILABLE",
                "Document storage is temporarily unavailable",
                status.HTTP_503_SERVICE_UNAVAILABLE,
                retryable=True,
            ) from exc

    def delete(self, object_name: str) -> None:
        try:
            self._bucket.blob(object_name).delete()
        except NotFound:
            return
        except (GoogleAPIError, GoogleAuthError) as exc:
            raise AppException(
                "SERVICE_UNAVAILABLE",
                "Could not clean up an unreferenced document",
                status.HTTP_503_SERVICE_UNAVAILABLE,
                retryable=True,
            ) from exc
