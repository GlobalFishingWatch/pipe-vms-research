"""
Executes a query to create the VMS summarazed tables.

This script will do:
1- Applies the jinja templating to the research positions query.
2- Creates the destination table in case it doesnt exist.
3- Run the query and save results in destination table.
"""

from datetime import datetime, timedelta
from enum import Enum
import os
import sys
from jinja2 import Environment, FileSystemLoader
import json
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import argparse, time, logging

from pipe_vms_research.utils.bqtools import BQTools
from pipe_vms_research.utils.ver import get_pipe_ver

logger = logging.getLogger()

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)+1):
        yield start_date + timedelta(n)

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

class RunMode(Enum):
    daily = 1
    batch = 2

    def __str__(self):
        return self.name

    @staticmethod
    def from_string(s):
        try:
            return RunMode[s]
        except KeyError:
            raise ValueError()

def run_research_positions(arguments):
    parser = argparse.ArgumentParser(description='Generates the research summarized tables.')
    parser.add_argument('-i','--source_table', help='The BQ source table  (Format str, ex: project:dataset.table)', required=True)
    parser.add_argument('-o','--destination_table', help='The BQ destination table  (Format str, ex: project:dataset.table).', required=True)
    parser.add_argument('-dr','--date_range', help='The date range to be processed (Format str YYYY-MM-DD[,YYYY-MM-DD]).', required=True)
    parser.add_argument('-sd', '--sunrise_dataset_table', help='The BQ table used as static sunrise  (Format str, ex: project:dataset.table).', required=False, 
                        default='world-fishing-827.pipe_static.sunrise')
    parser.add_argument('-labels','--labels', help='Adds a labels to a table (Format: json).', required=True, type=json.loads)
    parser.add_argument('-dry', '--dry_run', type=str2bool, nargs='?',
                        const=True, default=False,
                        help="Estimates the query execution cost without executing it.")
    parser.add_argument('-mode', '--mode', type=RunMode.from_string,
                        choices=list(RunMode),  default='batch',
                        required=True,
                        help="Execution mode: [ " +
                        "*'batch' Runs a single query for the entire period (default), " + \
                        "* 'daily' Runs one query per day in the period" + \
                        "]")


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

    destination_project, destination_ds_tl = args.destination_table.split(':')
    destination_table=args.destination_table.replace(':','.')
    source=f'{args.source_table}*'.replace(':','.')
    sunrise_dataset_table=args.sunrise_dataset_table.replace(':','.')
    dry_run = args.dry_run
    execution_mode = args.mode

    # when project is not set in the enviroment use the destination table project
    if not os.environ.get('GOOGLE_CLOUD_PROJECT'):
        os.environ['GOOGLE_CLOUD_PROJECT']=destination_project

    labels=args.labels

    bq_tools = BQTools()
    bq_tools.bq_client.project = destination_project
    description = f"""
        Created by pipe-vms-research: {get_pipe_ver()}.
        * Research positions generator process table
        * https://github.com/GlobalFishingWatch/pipe-vms-research
        * Source: {source}
        * Sunrise Table: {sunrise_dataset_table}
        * Date: {date_from.strftime('%Y-%m-%d')},{date_to.strftime('%Y-%m-%d')}
    """
    if not dry_run:
        logger.info(f'Creates the research daily table <{args.destination_table}> if it does not exists')
        schema = bq_tools.schema_json2builder('./assets/schemas/research_positions_schema.json')
        bq_tools.create_tables_if_not_exists(destination_table=destination_ds_tl,
                                             date_from=date_from,
                                             date_to=date_to,
                                             labels=labels,
                                             table_desc=description,
                                             schema=schema,
                                             clustering_fields=['ssvid'],
                                             date_field='timestamp')

    total_cost = 0
    try:
        if execution_mode == RunMode.daily:
            template = env.get_template('research_positions_daily.sql.j2')
            for date_x in daterange(date_from, date_to):
                query = template.render({
                    'date': date_x.strftime('%Y-%m-%d'),
                    'source': source,
                    'static_sunrise_dataset_and_table': sunrise_dataset_table,
                })
                # Run query to calc how much bytes will spend
                query_job=bq_tools.run_estimation_query(query, destination_table, labels)
                # Run query and calc research positions
                if dry_run:
                    total_cost=total_cost+query_job.total_bytes_processed
                else:
                    query_job=bq_tools.run_query(query, destination_table, labels)
                    total_cost=total_cost+query_job.total_bytes_processed

        elif execution_mode == RunMode.batch:
            template = env.get_template('research_positions_batch.sql.j2')
            query = template.render({
                'date_from': date_from.strftime('%Y-%m-%d'),
                'date_to': date_to.strftime('%Y-%m-%d'),
                'source': source,
                'static_sunrise_dataset_and_table': sunrise_dataset_table,
            })
            # Run query to calc how much bytes will spend
            query_job=bq_tools.run_estimation_query(query, destination_table, labels)
            # Run query and calc research positions
            if dry_run:
                total_cost=total_cost+query_job.total_bytes_processed
            else:
                query_job=bq_tools.run_query(query, destination_table, labels)
                total_cost=total_cost+query_job.total_bytes_processed

    except Exception as err:
        logger.error(f'Unrecongnized error: {err}.')
        sys.exit(1)

    if not dry_run:
        bq_tools.update_table_descr(destination_ds_tl, description)

    ### ALL DONE
    if dry_run:
        logger.info(f'All done. Total execution estimate: {total_cost/pow(1024,3):#.2f} GB')
    else:
        logger.info(f'All done, you can find the output ({date_from.strftime("%Y%m%d")}-{date_to.strftime("%Y%m%d")}): {destination_table}')
        logger.info(f'Total execution cost: {total_cost/pow(1024,3):#.2f} GB')
    logger.info(f'Execution time: {(time.time()-start_time)/60:#.2f} minutes')
