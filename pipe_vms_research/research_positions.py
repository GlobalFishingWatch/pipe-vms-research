"""
Executes a query to create the VMS summarazed tables.

This script will do:
1- Applies the jinja templating to the research positions query.
2- Creates the destination table in case it doesnt exist.
3- Run the query and save results in destination table.
"""

import argparse
import json
import logging
import time
from datetime import datetime, timedelta

from jinja2 import Environment, FileSystemLoader

from pipe_vms_research.utils.bqtools import BQTools, validate_bq_table
from pipe_vms_research.utils.ver import get_pipe_ver

logger = logging.getLogger()

BQ_TABLE_HELP = 'Format str, ex: [project:|.]dataset.table'
DATERANGE_HELP = (
    'The date range to be processed (Format str YYYY-MM-DD[,YYYY-MM-DD])'
)


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def run_research_positions(arguments):
    parser = argparse.ArgumentParser(
        description='Generates the research summarized tables.'
    )
    parser.add_argument(
        '-i',
        '--source_table',
        help=f'The BQ source table  ({BQ_TABLE_HELP})',
        required=True,
        type=validate_bq_table,
    )
    parser.add_argument(
        '-o',
        '--destination_table',
        help=f'The BQ destination table  ({BQ_TABLE_HELP}).',
        required=True,
        type=validate_bq_table,
    )
    parser.add_argument(
        '-dr', '--date_range', help=DATERANGE_HELP, required=True
    )
    parser.add_argument(
        '-sd',
        '--sunrise_dataset_table',
        help=f'The BQ static sunrise table ({BQ_TABLE_HELP}).',
        required=False,
        default='world-fishing-827:pipe_static.sunrise',
        type=validate_bq_table,
    )
    parser.add_argument(
        '-labels',
        '--labels',
        help='Adds a labels to a table (Format: json).',
        required=True,
        type=json.loads,
    )
    parser.add_argument(
        '-ep',
        '--executor_project_id',
        help='The BQ executor project ID',
        required=True,
        type=str,
    )
    args = parser.parse_args(arguments)

    start_time = time.time()

    date_range = args.date_range.split(',')
    date_from = datetime.strptime(date_range[0], '%Y-%m-%d')
    if len(date_range) > 1:
        date_to = datetime.strptime(date_range[1], '%Y-%m-%d')
    else:
        date_to = date_from + timedelta(days=1)

    # Apply template
    env = Environment(loader=FileSystemLoader('./assets/queries/'))

    bq_tools = BQTools(executor_project_id=args.executor_project_id)

    (destination_project, destination_ds, destination_tl) = (
        args.destination_table
    )
    destination_ds_tl = f'{destination_ds}.{destination_tl}'
    destination_table = (
        f'{destination_project or bq_tools.bq_client.project}'
        f'.{destination_ds}.{destination_tl}'
    )

    (source_project, source_ds, source_tl) = args.source_table
    source = (
        f'{source_project or bq_tools.bq_client.project}'
        f'.{source_ds}.{source_tl}*'
    )

    (sunrise_dataset_project, sunrise_dataset_ds, sunrise_dataset_tl) = (
        args.sunrise_dataset_table
    )
    sunrise_dataset_table = (
        f'{sunrise_dataset_project or bq_tools.bq_client.project}'
        f'.{sunrise_dataset_ds}.{sunrise_dataset_tl}'
    )

    # override destination project if provided
    bq_tools.bq_client.project = (
        destination_project or bq_tools.bq_client.project
    )

    labels = args.labels

    description = f"""
        Created by pipe-vms-research: {get_pipe_ver()}.
        * Research positions generator process table
        * https://github.com/GlobalFishingWatch/pipe-vms-research
        * Source: {source}
        * Sunrise Table: {sunrise_dataset_table}
        * Date: {date_from.strftime('%Y-%m-%d')},{date_to.strftime('%Y-%m-%d')}
    """
    logger.info(
        f'Creates the research daily table <{args.destination_table}>'
        ' if it does not exists'
    )
    schema = bq_tools.schema_json2builder(
        './assets/schemas/research_positions_schema.json'
    )
    bq_tools.create_tables_if_not_exists(
        destination_table=destination_table,
        date_from=date_from,
        date_to=date_to,
        labels=labels,
        table_desc=description,
        schema=schema,
        clustering_fields=['ssvid'],
        date_field='timestamp',
    )

    logger.info('Run query to populate research positions')
    total_cost = 0
    template = env.get_template('research_positions.sql.j2')
    query = template.render(
        {
            'date_from': date_from.strftime('%Y-%m-%d'),
            'date_to': date_to.strftime('%Y-%m-%d'),
            'source': source,
            'static_sunrise_dataset_and_table': sunrise_dataset_table,
        }
    )
    # Run query and calc research positions
    query_job = bq_tools.run_query(query, destination_table, labels)
    total_cost = total_cost + query_job.total_bytes_processed

    bq_tools.update_table_descr(destination_table, description)

    # ALL DONE
    logger.info(
        f'All done, you can find the output ({date_from.strftime("%Y%m%d")}-'
        f'{date_to.strftime("%Y%m%d")}): {destination_table}'
    )
    logger.info(f'Total execution cost: {total_cost/pow(1024, 3):#.2f} GB')
    logger.info(f'Execution time: {(time.time()-start_time)/60:#.2f} minutes')
