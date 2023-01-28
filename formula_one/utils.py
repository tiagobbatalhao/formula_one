from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account
from pathlib import Path
from loguru import logger


class BigQuery:
    def __init__(
        self,
        dataset: str,
        project: str,
        credentials_path: str,
    ):
        credentials = service_account.Credentials.from_service_account_file(
            Path(credentials_path).expanduser(),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.project = project
        self.dataset = dataset
        self.client = bigquery.Client(
            project=project,
            credentials=credentials,
        )

    def run_query(self, query, as_pandas=True, **query_params):
        query = query.format(
            project="`" + self.project + "`",
            dataset="`" + self.dataset + "`",
            **query_params,
        )
        df = self.client.query(query)
        if as_pandas:
            df = df.to_dataframe()
        return df

    def write_data(self, dataframe, table_name, schema, overwrite=False):
        client = self.client
        full_name = "{}.{}.{}".format(self.project, self.dataset, table_name)
        schema = [
            bigquery.SchemaField(field[0], field[1], field[2])
            for field in schema
        ]
        try:
            table = client.get_table(full_name)
        except NotFound:
            table = bigquery.Table(full_name, schema=schema)
            table = client.create_table(table, exists_ok=True)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE" if overwrite else "WRITE_APPEND",
            schema=table.schema,
        )
        df_save = dataframe.reset_index()[[c.name for c in schema]]
        logger.info("Writing {} rows to table {}".format(
            len(df_save),
            table.full_table_id,
        ))
        ret = client.load_table_from_dataframe(
            dataframe=df_save, destination=table, job_config=job_config
        )
        return ret

    def delete_table(self, table_name):
        client = self.client
        full_name = "{}.{}.{}".format(self.project, self.dataset, table_name)
        client.delete_table(full_name, not_found_ok=True)

