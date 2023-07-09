from typing import Any, Optional, Tuple

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


class GCPCredentials:
    _hook: Optional[BigQueryHook] = None
    _project_id: Optional[str] = None
    _credentials: Optional[Any] = None

    def __ensure_loaded(self) -> Tuple[Any, str]:
        """Busca la conexion `google_cloud_default` en airflow y devuelve las credenciales
        y `project_id` configurados en la conexion.

        Returns:
            Tuple[Any, str]: Credenciales y `project_id` configurados en la conexion.
        """
        if self._hook is None:
            self._hook = BigQueryHook(gcp_conn_id="google_cloud_default")
            (
                self._credentials,
                self._project_id,
            ) = self._hook._get_credentials_and_project_id()

        assert self._hook is not None
        assert self._project_id is not None
        assert self._credentials is not None

        return self._credentials, self._project_id

    @property
    def credentials(self) -> Any:
        """Devuelve las credenciales configuradas en la conexion `google_cloud_default`.

        Returns:
            Any: Credenciales configuradas en la conexion.
        """
        credentials, _ = self.__ensure_loaded()
        return credentials

    @property
    def project_id(self) -> str:
        """Devuelve el `project_id` configurado en la conexion `google_cloud_default`.

        Returns:
            str: `project_id` configurado en la conexion.
        """
        _, project_id = self.__ensure_loaded()
        return project_id


# Convertimos a singleton sobreescribiendo la clase para que no pueda ser instanciada
GCPCredentials = GCPCredentials()  # type: ignore
