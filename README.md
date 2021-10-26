# pipe-vms-research
Handles the creation of the summarized research tables for VMS.

## Structure

The `assets` forlder has the queries and schemas needed to generate the summaryized positions.
The `pipe_vms_research` folder has the code that loads the queries and upload the data to BQ.

## Requirements

.

## Structure

The `assets` forlder has the queries and schemas needed to generate the summaryized positions.
The `pipe_vms_research` folder has the code that loads the queries and upload the data to BQ.

## Requirements

You need to have installed:
- `docker`

## Run

```bash
$ python -m pipe_vms_research.research_positions -i <source_table> -o <destination_table> -dr <date_range>
```


