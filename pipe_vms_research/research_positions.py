"""
Executes a query to create the VMS summarazed tables.

This script will do:
1- Applies the jinja templating to the research positions query.
2- Creates the destination table in case it doesnt exist.
3- Run the query and save results in destination table.
"""

from datetime import datetime, timedelta
from jinja2 import Environment, FileSystemLoader
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import argparse, time

def create_table_if_not_exists(client, destination_table_ref):
    """Creates table if it does not exists

    :param client: Client of BQ.
    :type client: BigQuery.Client
    :param destination_table_ref: Reference of a Table in BQ.
    :type destination_table_ref: BigQuery.TableReference
    """
    try:
        table = client.get_table(destination_table_ref) #API request
    except NotFound:
        schema = [
            bigquery.SchemaField('lat', 'FLOAT', description='The latitude where the vessel was positioned.'),
            bigquery.SchemaField('lon', 'FLOAT', description='The longitude where the vessel was positioned.'),
            bigquery.SchemaField('speed', 'FLOAT', description='The speed of the vessel.'),
            bigquery.SchemaField('course', 'FLOAT', description='The course of the vessel.'),
            bigquery.SchemaField('timestamp', 'TIMESTAMP', description='The moment when was capture the position.'),
            bigquery.SchemaField('hours', 'FLOAT', description='The fishing hours.'),
            bigquery.SchemaField('nnet_score', 'FLOAT', description='The score of neural network to indicate that was fishing.'),
            bigquery.SchemaField('seg_id', 'STRING', description='The segment identification.'),
            bigquery.SchemaField('ssvid', 'STRING', description='The ssvid.'),
            bigquery.SchemaField('distance_from_port_m', 'FLOAT', description='The distance from port.'),
            bigquery.SchemaField('distance_from_shore_m', 'FLOAT', description='The distance from shore.'),
            bigquery.SchemaField('elevation_m', 'FLOAT', description='The elevation.'),
            bigquery.SchemaField('source', 'STRING', description='The source which the message belongs.')
        ]
        table = bigquery.Table(destination_table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_ = bigquery.TimePartitioningType.MONTH,
            field = "timestamp",  # name of column to use for partitioning
        )
        table.clustering_fields = ["timestamp"]
        table = client.create_table(table)



def delete_partition(client, destination_table, date_from, date_to):
    query_job = client.query(f"""
        DELETE FROM `{ destination_table }`
        WHERE DATE(timestamp) >= '{ date_from }' AND DATE(timestamp) < '{ date_to }'
     """, bigquery.QueryJobConfig(use_query_cache=False,use_legacy_sql=False))
    result = query_job.result()
    print(f'delete_partition result: {result}')


def run_research_positions(arguments):
    parser = argparse.ArgumentParser(description='Generates the research summarized tables.')
    parser.add_argument('-i','--source_table', help='The BQ source table (Format str, ex: datset.table).', required=True)
    parser.add_argument('-o','--destination_table', help='The BQ destination table (Format str, ex: datset.table).', required=True)
    parser.add_argument('-dr','--date_range', help='The date range to be processed (Format str YYYY-MM-DD[,YYYY-MM-DD]).', required=True)

    args = parser.parse_args(arguments)

    start_time = time.time()

    date_range = args.date_range.split(',')
    date_from = datetime.strptime(date_range[0], '%Y-%m-%d')
    if len(date_range)>1:
        date_to = datetime.strptime(date_range[1], '%Y-%m-%d')
    else:
        date_to = date_from+timedelta(days=1)

    # Apply template
    env = Environment(loader=FileSystemLoader('./assets/queries/'))
    template = env.get_template('research_positions_query.sql.j2')
    query = template.render(date_from=date_from.strftime('%Y%m%d'), date_to=date_to.strftime('%Y%m%d'), source=f'{args.source_table}*')


    client = bigquery.Client(project='world-fishing-827')
    #Creates partitioned table
    destination_table = args.destination_table.split('.')
    destination_dataset_ref = bigquery.DatasetReference(client.project, destination_table[0])
    destination_table_ref = destination_dataset_ref.table(destination_table[1])
    create_table_if_not_exists(client, destination_table_ref)
    delete_partition(client, args.destination_table, date_from.strftime('%Y-%m-%d'), date_to.strftime('%Y-%m-%d'))

    # Run query to calc how much bytes will spend
    job_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
        priority=bigquery.QueryPriority.BATCH,
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        destination=f'{client.project}.{args.destination_table}'
    )

    query_job = client.query(query, job_config=job_config)  # Make an API request.
    print(f'This query will process {query_job.total_bytes_processed} bytes.')

    # Run query and calc research positions
    job_config = bigquery.QueryJobConfig(
        use_query_cache=False,
        priority=bigquery.QueryPriority.BATCH,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        destination=f'{client.project}.{args.destination_table}'
    )

    query_job = client.query(query, job_config=job_config)  # Make an API request.
    print(f'Job {query_job.job_id} is currently in state {query_job.state}')

    query_job.result() # Wait for the job to complete.


    ### ALL DONE
    print(f'All done, you can find the output ({date_from.strftime("%Y%m%d")}-{date_to.strftime("%Y%m%d")}): {args.destination_table}')
    print(f'Execution time {(time.time()-start_time)/60} minutes')
