from dataclasses import dataclass
from typing import TYPE_CHECKING, Sequence
from airflow.models.baseoperator import BaseOperator


if TYPE_CHECKING:
    from airflow.utils.context import Context

@dataclass
class DataSource:
    location: str
    conn_id: str

@dataclass
class TargetTable:
    name: str

@dataclass
class StagingTable:
    name: str
    
class LoadOperator(BaseOperator):
    ui_color: str = "#16239E"
    ui_fgcolor: str = "#EBC950"
    template_fields: Sequence[str] = (
        'source',
        'watermark',
    )

    def __init__(
        self,
        *,
        source: dict,
        target_table: dict,
        staging_table: dict,
        watermark: dict,
        conn_id: str = 'redshift_default',
    ):
        self.source = source,
        self.target_table = target_table
        self.staging_table = staging_table
        self.watermark = watermark
        self.conn_id = conn_id

    def execute(self, context: 'Context'):
        source = DataSource(**self.source)
        staging_table = StagingTable(**self.staging_table)
        target_table = TargetTable(**self.target_table)

        self.log.info('Loading data from source %s to %s', source.location, staging_table.name)

        self.log.info('Loading data from staging table %s to target table %s', staging_table.name, target_table.name)
