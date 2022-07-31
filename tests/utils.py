from sqlalchemy.orm.session import Session
from airflow.utils.session import NEW_SESSION, create_session, provide_session  # noqa: F401
from airflow.models.connection import Connection

@provide_session
def merge_conn(conn, session: Session = NEW_SESSION):
    """Add new Connection."""
    if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
        session.add(conn)
        session.commit()
