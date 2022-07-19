# LARD

> Dictionary definition
>  insert strips of fat or bacon in (meat) before cooking.

Lard provides the capability to load data into Airflow in an idempotent manner.

## How it works

LARD watermarks loads the data in the following sequence:

* deletes records with the given watermark existing in the target table.
* loads data into a temporary staging table.
* copies data from the temporary staging table to the target table.

# Installation

    # Upgrade pip if necessary
    python -m pip install --upgrade pip

    # Install package
    pip install lard


# Usage

The following is an example of a load operation of data from an S3 bucket location to the `events` table of the `lards` schema in the Redshift database.

```python
import lard
from airflow import DAG

dag = DAG('sample_dag')
lard_task = lard.LoadOperator(
    'lard_events',
    dag=dag,
    conn_id='redshift',
    source=dict(
        location="s3://event-logs/{{ data_interval_start.strftime('%Y/%m/%d/%H') }}/",
        conn_id='s3_default'
    ),
    target_table=dict(
        name='sample.events'
    ),
    staging_table=dict(
        name='events_staging',
    ),
    watermark=dict(
        target_name='scheduled_at',
        data_type='TIMESTAMP',
        source_value='{{ data_interval_end | ts }}'
    )
)
```