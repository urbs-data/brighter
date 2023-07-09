import logging
from typing import Optional

import pandas as pd

from brighter.gcp.credentials import GCPCredentials

LOGGER = logging.getLogger(__name__)


def df_to_gbq(
    df: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    if_exists="append",
    fix_dtypes=True,
) -> None:
    """Guarda un DataFrame en BigQuery utilizando la conexion default de Airflow con Google Cloud. Si no existe el
    dataset o la tabla, los crea automaticamente utilizando el schema del DF. En caso de que exista la tabla, realiza
    un append de los datos.

    Args:
        df (pd.DataFrame): DataFrame a guardar.
        dataset_id (str): Nombre del dataset a usar.
        table_id (str): Nombre de la tabla a insertar datos.
        if_exists (str): Accion a tomar cunado la tabla existe. Default 'append'.
        fix_dtypes (bool): Determina si las columnas objects se pasaran a str antes de subir a GBQ. Default True.
    """
    import pandas_gbq

    if fix_dtypes:
        # Seteo explicitamente el tipo str por si hay mix de tipos dentro de las columnas object. En caso de no hacerlo,
        # cuando se quiere subir a gcp, pyarrow rompe.
        columnas_str = df.select_dtypes("object").columns
        for columna_str in columnas_str:
            df[columna_str] = df[columna_str].astype(str)

    # Seteamos las columnas sin espacios porque en GBQ no se pueden tener columnas con espacios.
    df.columns = [column.replace(" ", "_") for column in df.columns]

    pandas_gbq.to_gbq(
        df,
        f"{dataset_id}.{table_id}",
        if_exists=if_exists,
        project_id=GCPCredentials.project_id,
        credentials=GCPCredentials.credentials,
    )


def gbq_to_df(
    dataset_id: Optional[str] = None,
    table_id: Optional[str] = None,
    columns: Optional[str] = "*",
    where_filter: Optional[str] = None,
    sql_query: Optional[str] = None,
) -> pd.DataFrame:
    """Ejecuta una consulta a GBQ y la devuelve como dataframe. Puede ejecutar una query con el parametro `sql_query`
    o puede seleccionar una tabla entera.

    Args:
        dataset_id (Optional[str], optional): Id del dataset en caso de consultar una tabla. Defaults to None.
        table_id (Optional[str], optional): Id de la tabla a consultar. Defaults to None.
        columns (Optional[str], optional): Columnas a seleccionar de la tabla. Defaults to "*".
        where_filter (Optional[str], optional): Filtros a aplicar de la tabla. Defaults to None.
        sql_query (Optional[str], optional): Query general a ejecutar. Defaults to None.

    Returns:
        pd.DataFrame: Datos de la consulta a GCP.
    """
    import pandas_gbq

    if sql_query is None:
        query_or_table = f"SELECT {columns} FROM {dataset_id}.{table_id}"
        if where_filter is not None:
            query_or_table = f"{query_or_table} WHERE {where_filter}"
    else:
        query_or_table = sql_query

    LOGGER.info("Querying: %s", query_or_table)

    df_query = pandas_gbq.read_gbq(
        query_or_table=query_or_table,
        project_id=GCPCredentials.project_id,
        credentials=GCPCredentials.credentials,
        progress_bar_type="None",
    )

    assert type(df_query) is pd.DataFrame

    return df_query


def check_if_table_exists(dataset_id: str, table_id: str) -> bool:
    """Chequea que la tabla exista en BigQuery.

    Args:
        dataset_id (str): Id del dataset.
        table_id (str): Id de la tabla a chequear.

    Returns:
        bool: Si existe la tabla en BigQuery.
    """
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound

    client = bigquery.Client(credentials=GCPCredentials.credentials)
    try:
        client.get_table(f"{GCPCredentials.project_id}.{dataset_id}.{table_id}")
        return True
    except NotFound:
        return False


def run_query_and_load_to_gbq(
    sql_query: str,
    dataset_id: str,
    table_id: str,
    if_exists: str,
) -> None:
    """Ejecuta una query a GCP y guarda los resultados en una tabla.

    Args:
        sql_query (str): Query a ejecutar.
        dataset_id (str): Id del dataset a guardar.
        table_id (str): Id de la tabla donde se guardaran los resultados.
        if_exists (str): Politica a aplicar si la tabla de resultados ya existe.
    """
    from brighter.gcp.bigquery import df_to_gbq, gbq_to_df

    mid_clientes = gbq_to_df(sql_query=sql_query)

    df_to_gbq(
        mid_clientes,
        dataset_id=dataset_id,
        table_id=table_id,
        if_exists=if_exists,
    )
