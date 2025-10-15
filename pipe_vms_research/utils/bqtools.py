import argparse
import json
import logging
import re
import sys
from datetime import datetime

from google.api_core.exceptions import BadRequest
from google.api_core.exceptions import Conflict as AlreadyExistErr
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger()


def validate_bq_table(input_string):
    # Define the regex pattern for validation
    pattern = r"^([^:\.]+[:\.])?([^:\.]+\.[^:\.]+)$"

    # Perform regex matching
    match = re.match(pattern, input_string)

    if match:
        project_id, dataset_table = match.group(1, 2)
        dataset_name, table_name = (
            dataset_table.split(".")
            if dataset_table.count(".") == 1
            else (None, None)
        )

        return (
            project_id[:-1] if project_id else None,
            dataset_name if dataset_name else None,
            table_name if table_name else None,
        )
    else:
        raise argparse.ArgumentTypeError("Invalid BQ table format.")


class BQTools:

    def __init__(self, executor_project_id: str = None):
        self.bq_client = bigquery.Client(project=executor_project_id)

    def schema_json2builder(self, schema_path: str):
        """
        Reads json schema and convert to array of bigquery.SchemaFields.
        :param schema_path: The path to the schema.
        :type schema_path: str.
        """
        schema = None
        with open(schema_path) as schemafield:
            columns = json.load(schemafield)
            schema = list(
                map(
                    lambda c: bigquery.SchemaField(
                        c["name"],
                        c["type"],
                        mode=c["mode"],
                        description=c["description"],
                        fields=(
                            [
                                bigquery.SchemaField(
                                    f["name"],
                                    f["type"],
                                    mode=f["mode"],
                                    description=f["description"],
                                )
                                for f in c["fields"]
                            ]
                            if c["type"].upper() == "RECORD"
                            else []
                        ),
                    ),
                    columns,
                )
            )
        return schema

    def create_tables_if_not_exists(
        self,
        destination_table: str,
        date_from: datetime,
        date_to: datetime,
        labels,
        table_desc: str,
        schema: list,
        clustering_fields: list = [],
        date_field: str = "timestamp",
        partitioning_enforcement_enabled: bool = True,
    ):
        """Creates tables if they do not exists.
        If it doesn't exist, create it. And if exists, deletes the data of
        date range.

        :param destination_table: project.dataset.table of BQ.
        :type destination_table: str.
        :param date_from: the date from.
        :type date_from: datetime.
        :param date_to: the date to.
        :type date_to: datetime.
        :param labels: the label of the dataset. Default None.
        :type labels: dict.
        :param table_desc: the main description of the table.
        :type table_desc: str.
        :param schema: the schema of the table.
        :type schema: list[bigquery.SchemaField].
        :param clustering_fields: the clustering fields of the table.
        :type clustering_fields: list[str]. Default: [].
        :param date_field: the date field use to check the from and to dates.
        :type date_field: str. Default timestamp.
        """
        (dst_project, dst_dataset, dst_table) = validate_bq_table(
            destination_table
        )
        dst_dataset_ref = bigquery.DatasetReference(dst_project, dst_dataset)
        dst_table_ref = dst_dataset_ref.table(dst_table)
        try:
            table = self.bq_client.get_table(dst_table_ref)  # API request
            logger.info(f"Ensures the table [{table}] exists.")
            query_job = self.bq_client.query(
                f"""
                   DELETE FROM `{destination_table}`
                   WHERE date({date_field}) BETWEEN '{date_from:%Y-%m-%d}'
                   AND '{date_to:%Y-%m-%d}'
                """,
                bigquery.QueryJobConfig(
                    use_query_cache=False,
                    use_legacy_sql=False,
                    labels=labels,
                ),
            )
            logger.info(
                f"Delete Job {query_job.job_id} is currently "
                f"in state {query_job.state}"
            )
            result = query_job.result()
            logger.info(
                f"Date range [{date_from:%Y-%m-%d},{date_to:%Y-%m-%d}] "
                f"cleaned: {result}"
            )

        except BadRequest as err:
            logger.error(
                f"create_tables_if_not_exists - Bad request received {err}."
            )

        except NotFound:
            logger.info(dst_table_ref.project)
            if not dst_table_ref.project:
                dst_table_ref.project = self.bq_client.project

            table = bigquery.Table(dst_table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field=date_field,
            )
            table.require_partition_filter = partitioning_enforcement_enabled
            clustering_fields.insert(0, date_field)
            table.clustering_fields = clustering_fields
            table.description = table_desc
            table.labels = labels
            table = self.bq_client.create_table(table)
            logger.info(f"Table {dst_dataset}.{dst_table} created.")

        except Exception as err:
            logger.error(
                f"create_tables_if_not_exists - Unrecongnized error: {err}."
            )
            sys.exit(1)

    def create_table(
        self,
        destination_table: str,
        labels,
        table_desc: str,
        schema: list,
        clustering_fields: list = None,
    ):
        """Creates the table if they do not exists.
        If it doesn't exist, create it. And if exists, returns error.

        :param destination_table: project.dataset.table of BQ.
        :type destination_table: str.
        :param labels: the label of the dataset. Default None.
        :type labels: dict.
        :param schema: the schema of the table.
        :type schema: list[bigquery.SchemaField].
        """
        (dst_project, dst_dataset, dst_table) = validate_bq_table(
            destination_table
        )
        dst_dataset_ref = bigquery.DatasetReference(dst_project, dst_dataset)
        dst_table_ref = dst_dataset_ref.table(dst_table)
        try:
            table = bigquery.Table(dst_table_ref, schema=schema)
            table.description = table_desc
            table.labels = labels
            table.clustering_fields = clustering_fields
            table = self.bq_client.create_table(table)
            logger.info(
                f"Table {dst_dataset}.{dst_table} "
                "created with specific schema."
            )
        except BadRequest as err:
            logger.error(f"create_table - Bad request received {err}.")
            sys.exit(1)
        except AlreadyExistErr as err:
            logger.warn(f"Already exists table: {err}.")
        except Exception as err:
            logger.error(f"create_table - Unrecongnized error: {err}.")
            sys.exit(1)

    def update_table(self, destination_table, description, schema):
        """Updates the schema of an existent table.

        :param destination_table: project.dataset.table of BQ.
        :type destination_table: str.
        :param description: the main description of the table.
        :type description: str.
        :param schema: the schema of the table.
        :type schema: list[bigquery.SchemaField].
        """
        (dst_project, dst_dataset, dst_table) = validate_bq_table(
            destination_table
        )
        dst_dataset_ref = bigquery.DatasetReference(dst_project, dst_dataset)
        dst_table_ref = dst_dataset_ref.table(dst_table)
        try:
            table = self.bq_client.get_table(dst_table_ref)
            table.schema = schema
            table.description = description
            result = self.bq_client.update_table(
                table, ["description", "schema"]
            )
            logger.info(
                "Update table schema from table "
                f" {dst_dataset}.{dst_table}. Result: {result}"
            )
        except BadRequest as err:
            logger.error(f"update_table - Bad request received {err}.")
            sys.exit(1)
        except Exception as err:
            logger.error(f"update_table - Unrecongnized error: {err}.")
            sys.exit(1)

    def update_table_descr(self, destination_table, description):
        """Updates the schema of an existent table.

        :param destination_table: project.dataset.table of BQ.
        :type destination_table: str.
        :param description: the main description of the table.
        :type description: str.
        """
        (dst_project, dst_dataset, dst_table) = validate_bq_table(
            destination_table
        )
        dst_dataset_ref = bigquery.DatasetReference(dst_project, dst_dataset)
        dst_table_ref = dst_dataset_ref.table(dst_table)
        try:
            table = self.bq_client.get_table(dst_table_ref)
            table.description = description
            result = self.bq_client.update_table(table, ["description"])
            logger.info(
                "Update table description from table "
                f"{dst_dataset}.{dst_table}. Result: {result}"
            )
        except BadRequest as err:
            logger.error(f"update_table_descr - Bad request received {err}.")
            sys.exit(1)
        except Exception as err:
            logger.error(f"update_table_descr - Unrecongnized error: {err}.")
            sys.exit(1)

    def run_query(
        self, query, destination, labels, is_partitioned: bool = True
    ):
        """Runs the query using the client.

        :param query: The query.
        :type query: str.
        :param destination_table: project.dataset.table of BQ.
        :type destination_table: str.
        :param labels: the label of the dataset. Default None.
        :type labels: dict.
        :param estimate: If wants to get the estimation of the query.
        :type estimate: bool.
        """
        job_config = bigquery.QueryJobConfig(
            dry_run=False,
            use_query_cache=False,
            priority=bigquery.QueryPriority.BATCH,
            use_legacy_sql=False,
            write_disposition=(
                "WRITE_APPEND" if is_partitioned else "WRITE_TRUNCATE"
            ),
            destination=destination,
            labels=labels,
        )

        logger.info(f"Execute real BATCH query, destination {destination}")
        try:
            query_job = self.bq_client.query(
                query, job_config=job_config
            )  # Make an API request.
            logger.info(
                f"Job {query_job.job_id} is currently in state {query_job.state}"
            )
            query_job.result()  # Wait for the job to complete.
            return query_job

        except Exception as err:
            logger.error(f"run_query - Unknown Error has occurred {err}.")
            sys.exit(1)
