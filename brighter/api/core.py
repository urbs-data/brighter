import abc
import logging
from datetime import datetime
from typing import Any, List, Optional

import pandas as pd
import requests
from airflow.models import Variable

from brighter.gcp.storage import json_to_gcs

LOGGER = logging.getLogger(__name__)
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


class Api(abc.ABC):
    base_url = ""
    default_headers = {}
    current_token: Optional[str] = None

    @classmethod
    def _save_logs_gcs(cls, response: dict, service_url: str):
        """
        Almacena en un bucket de gcs una response de la api.

        Args:
            response (dict): Es la response que devuelve el endpoint.
            service_url (str): Es el url puntual de ese enpoint.
        """
        today_date = datetime.now().strftime("%Y-%m-%d")
        today_timestamp = datetime.now().strftime("%H-%M-%s")
        bucket_id = Variable.get("bucket_logs_name")

        filename = f"{service_url.replace('/', '_')}_{today_timestamp}.json"
        json_to_gcs(
            response=response,
            bucket_id=bucket_id,
            folder_name=today_date,
            file_name=filename,
        )

    @classmethod
    def _make_get_request(cls, url: str) -> Any:
        """Realiza un GET. En caso de no estar logueado o haber vencido la sesion, se loguea.

        Args:
            url (str): Url a realizar el POST.

        Raises:
            ConnectionError: Cuando la API devuelve un error.

        Returns:
            Any: Respuesta del endpoint.
        """
        if cls.current_token is None:
            cls._login()

        api_url = f"{cls.base_url}{url}"
        response = requests.get(api_url, headers=cls.default_headers)

        if response.status_code == 401:
            cls._login()
            return cls._make_get_request(api_url)

        if 200 <= response.status_code <= 300:
            cls._save_logs_gcs(response.json(), url)
            return response.json()

        raise ConnectionError(response.text)

    @classmethod
    def _make_post_request(cls, url: str, body: Optional[dict]) -> Any:
        """Realiza un POST a ESCO. En caso de no estar logueado o haber vencido la sesion, se loguea.

        Args:
            url (str): Url a realizar el POST.
            body (Optional[dict]): Body a enviar en el POST.

        Raises:
            ConnectionError: Cuando la API devuelve un error.

        Returns:
            Any: Respuesta del endpoint.
        """
        if cls.current_token is None:
            cls._login()

        api_url = f"{cls.base_url}{url}"
        response = requests.post(
            api_url, json=body, headers=cls.default_headers
        )

        if response.status_code == 401:
            cls._login()
            return cls._make_post_request(api_url, body)

        if 200 <= response.status_code <= 300:
            cls._save_logs_gcs(response.json(), url)
            return response.json()

        raise ConnectionError(response.text)

    @classmethod
    def _parse_df(cls, datos: List[Any]) -> pd.DataFrame:
        """Transforma una lista de datos en un DataFrame con el campo `_batch_date` = ahora.

        Args:
            datos (List[Any]): Lista de datos a transformar en DataFrame.

        Returns:
            pd.DataFrame: DataFrame con los datos como registros.
        """
        df = pd.DataFrame(datos)
        df["_batch_date"] = datetime.now()
        return df

    @abc.abstractclassmethod
    def _login(cls) -> None:
        """Se autentica con la API. Guarda el `access_token` para ser usado en las siguientes llamadas."""
        ...
