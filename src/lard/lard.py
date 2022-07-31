from dataclasses import dataclass
from typing import TYPE_CHECKING, Sequence
from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from psycopg2.extensions import connection

if TYPE_CHECKING:
    from airflow.utils.context import Context


@dataclass
class DataSource:
    location: str
    conn_id: str

    format: str = ""
    pargs: str = ""

    def copy_to(self, destination: "StagingTable") -> str:
        return destination.copy_from(self)

    @property
    def authorization(self) -> str:
        conn = Connection(conn_id=self.conn_id)
        role_arn = conn.extra_dejson.get('role_arn', None)
        if role_arn:
            return f'iam_role {role_arn!r}'
        return ''

@dataclass
class Watermark:
    target_name: str
    source_value: str


@dataclass
class TableMixin:
    name: str
    hook: DbApiHook

    @property
    def conn_type(self):
        return self.hook.conn_type


class TargetTable(TableMixin):
    watermark: "Watermark"
    schema: str = "public"

    def copy_from(self, staging_table: "StagingTable") -> str:
        if self.conn_type == "redshift":
            return ""
        return ""

    @property
    def conn(self) -> "connection":
        return self.active_connection

    @conn.setter
    def conn(self, active_connection):
        self.active_connection = active_connection

    def delete(self, watermark: "Watermark") -> str:
        if self.conn_type == "redshift":
            return f"""
            DELETE {self.name}
                WHERE {watermark.target_name} = %(watermark_value)s;"""
        return ""

    def columns(self) -> str:
        if self.conn_type == "redshift":
            cols = []
            with self.conn as cur:
                # Sort the watermark column to the end of the result
                cur.execute(
                    """
                    SELECT column_name 
                        FROM information_schema.columns 
                        WHERE 
                            table_name=%(table_name)s 
                            AND table_schema=%(table_schema)s 
                            AND column_name!=%(watermark_name)s
                        ORDER BY ordinal_position
                    UNION
                    SELECT %(watermark_name)s;
                """,
                    table_name=self.name,
                    table_schema=self.schema,
                    watermark_name=self.watermark.target_name,
                )
                cols = [row[0] for row in cur.fetchall()]
                cols = ", ".join(cols)
            return cols
        return ""

    def columns_excluding_watermark(self) -> str:
        columns = self.columns.split(", ")[:-1]
        columns = ", ".join(columns)
        return columns


@dataclass
class StagingTable(TableMixin):
    def create_like(self, target_table: "TargetTable") -> str:
        if self.conn_type == "redshift":
            return f"""
                CREATE TEMP TABLE IF NOT EXISTS {self.name} 
                    (LIKE {self.target_table.name});
                """
        return ""

    def delete_column(self, watermark: "Watermark") -> str:
        if self.conn_type == "redshift":
            return f"ALTER TABLE {self.name} DROP COLUMN {watermark.target_name};"
        return ""

    def copy_from(self, source: "DataSource") -> str:
        if self.source_type == "s3":
            return f"""
            COPY {self.name}
                FROM {source.location!r} 
                {source.authorization}
                {source.format}
                {source.pargs};
            """
        return ""

    def copy_to(self, destination: "TargetTable") -> str:
        if self.conn_type == "redshift":
            return f"""
            INSERT INTO {destination.name}({destination.columns})
            SELECT {destination.columns_excluding_watermark}, %(watermark_value)s FROM {self.name}
            """
        return ""


class LoadOperator(BaseOperator):
    ui_color: str = "#16239E"
    ui_fgcolor: str = "#EBC950"
    template_fields: Sequence[str] = (
        "source",
        "watermark",
    )

    def __init__(
        self,
        *,
        source: dict,
        target_table: dict,
        staging_table: dict,
        watermark: dict,
        conn_id: str = "redshift_default",
    ):
        self.source = (source,)
        self.target_table = target_table
        self.staging_table = staging_table
        self.watermark = watermark
        self.conn_id = conn_id

    def execute(self, context: "Context"):
        source = DataSource(**self.source)
        hook = Connection(conn_id=self.conn_id).get_hook()
        watermark = Watermark(**watermark)
        staging_table = StagingTable(**self.staging_table, hook=hook)
        target_table = TargetTable(**self.target_table, hook=hook, watermark=watermark)

        with hook.get_conn() as conn:
            target_table.conn = conn
            query = staging_table.create_like(target_table)
            self.log.info(
                "Creating staging table %s query=%s", staging_table.name, query
            )
            conn.execute(query)
            query = staging_table.delete_column(watermark)
            self.log.info(
                "Dropping watermark from staging table %s query=%s",
                staging_table.name,
                query,
            )
            conn.execute(query)
            query = source.copy_to(staging_table)
            self.log.info(
                "Loading from datasource %s to staging table %s query=%s",
                source.location,
                staging_table.name,
                query,
            )
            conn.execute(query)
            query = target_table.delete(watermark)
            self.log.info(
                "Deleting data matching watermark from target table %s query=%s",
                target_table.name,
                query,
            )
            conn.execute(query, {"watermark_value": watermark.source_value})
            query = staging_table.copy_to(target_table)
            self.log.info(
                "Loading data from staging table %s to target table %s query=%s",
                staging_table.name,
                target_table.name,
                query,
            )
