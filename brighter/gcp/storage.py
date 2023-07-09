import io
from pathlib import Path
from typing import List

import pandas as pd
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.cloud.storage.bucket import Bucket

from brighter.gcp.credentials import GCPCredentials


def _get_bucket(bucket_id: str) -> Bucket:
    """Genera una conexion a GCS a partir de un bucket.

    Args:
        bucket_id (str): Es el nombre del bucket.

    Returns:
        Bucket: Bucket de GCS.
    """
    storage_client = storage.Client(
        project=GCPCredentials.project_id,
        credentials=GCPCredentials.credentials,
    )

    return storage_client.get_bucket(bucket_id)


def _get_blob(bucket_id: str, folder_name: str, file_name: str) -> Blob:
    """Genera una conexion a GCS a partir de un bucket, folder y archivo.

    Args:
        bucket_id (str): Es el nombre del bucket.
        folder_name (str): Es el nombre de la carpeta del bucket.
        file_name (str): Es el nombre del archivo.

    Returns:
        Blob: Blob del archivo en GCS.
    """
    bucket = _get_bucket(bucket_id)

    return bucket.blob(f"{folder_name}/{file_name}")


def list_files(bucket_id: str) -> List[Blob]:
    """Lista los archivos de un bucket.

    Args:
        bucket_id (str): Nombre del bucket.

    Returns:
        List[Blob]: Archivos del bucket.
    """
    bucket = _get_bucket(bucket_id)

    return [
        blob for blob in bucket.list_blobs() if Path(blob.name).suffix != ""
    ]


def json_to_gcs(
    response: dict,
    bucket_id: str,
    folder_name: str,
    file_name: str,
) -> None:
    """Guarda un json de una response a un bucket y una folder de GCS.

    Args:
        response (str): Es el json que queremos almacenar.
        bucket_id (str): Es el nombre del bucket donde queremos almacenar el json.
        folder_name (str): Es el nombre de la carpeta del bucket donde queremos almacenar el json.
        file_name (str): Es el nombre que le queremos dar al archivo.
    """
    blob = _get_blob(bucket_id, folder_name, file_name)

    blob.upload_from_string(data=str(response), content_type="application/json")


def df_to_gcs(
    df: pd.DataFrame,
    bucket_id: str,
    folder_name: str,
    file_name: str,
) -> None:
    """Guarda un DF en un bucket y una folder de GCS.

    Args:
        df (pd.DataFrame): DF a almacenar.
        bucket_id (str): Es el nombre del bucket donde queremos almacenar el json.
        folder_name (str): Es el nombre de la carpeta del bucket donde queremos almacenar el json.
        file_name (str): Es el nombre que le queremos dar al archivo.
    """
    blob = _get_blob(bucket_id, folder_name, file_name)

    with io.StringIO() as buffer:
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        blob.upload_from_file(buffer)
