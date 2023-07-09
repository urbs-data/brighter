import datetime as dt
from typing import Optional

from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

from brighter.gcp.bigquery import check_if_table_exists
from brighter.gcp.credentials import GCPCredentials


class BranchIfBigQueryTableExistsOperator(BranchPythonOperator):
    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        task_id_if_exists: str,
        task_id_if_not_exists: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            python_callable=lambda: task_id_if_exists
            if check_if_table_exists(dataset_id=dataset_id, table_id=table_id)
            else task_id_if_not_exists,
            **kwargs,
        )


class BackupBigQueryTableOperator(BigQueryInsertJobOperator):
    def __init__(
        self,
        dataset_id: str,
        table_id: str,
        date_from: dt.datetime,
        date_to: dt.datetime,
        date_column="_batch_date",
        **kwargs,
    ) -> None:
        query = f"SELECT * FROM {dataset_id}.{table_id} WHERE {date_column} >= '{date_from:%Y-%m-%d}' AND {date_column} <= '{date_to:%Y-%m-%d}'"

        super().__init__(
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                    "writeDisposition": "WRITE_EMPTY",
                    "destinationTable": {
                        "projectId": GCPCredentials.project_id,
                        "datasetId": dataset_id,
                        "tableId": f"temp_{table_id}",
                    },
                }
            },
            **kwargs,
        )


class DeleteRecordsFromBigQueryTableOperator(BigQueryInsertJobOperator):
    def __init__(
        self,
        dataset_id: str,
        table_id: str,
        date_from: dt.datetime,
        date_to: dt.datetime,
        date_column="_batch_date",
        **kwargs,
    ) -> None:
        query = f"DELETE FROM {dataset_id}.{table_id} WHERE {date_column} >= '{date_from:%Y-%m-%d}' AND {date_column} <= '{date_to:%Y-%m-%d}'"

        super().__init__(
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                }
            },
            **kwargs,
        )
