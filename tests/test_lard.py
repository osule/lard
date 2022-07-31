import pytest
import json
from sqlalchemy.orm.session import Session

from airflow.models.connection import Connection
from airflow.utils.session import NEW_SESSION, provide_session  # noqa: F401

from lard import DataSource
from utils import merge_conn


@pytest.fixture
@provide_session
def conn(session: Session = NEW_SESSION):
    merge_conn(
        Connection(
            conn_id="s3_default", extra=json.dumps(dict(role_arn="aws:iam:test-role"))
        ),
        session,
    )


def test_DataSource(conn):
    ds = DataSource(
        location="s3://events/2022/06/12/13/2012-02-23.log",
        conn_id="s3_default",
        format="json",
        pargs="",
    )
    assert ds.authorization == 'iam role "aws:iam:test-role"'
