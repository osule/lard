import os
import sys
import pytest

assert "airflow" not in sys.modules, "No airflow module can be imported before these lines"
tests_directory = os.path.dirname(os.path.realpath(__file__))

os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = os.path.join(tests_directory, "dags")
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AIRFLOW__WEBSERVER__DAG_DEFAULT_VIEW"] = "grid"
os.environ["AWS_DEFAULT_REGION"] = os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
os.environ["CREDENTIALS_DIR"] = os.environ.get('CREDENTIALS_DIR') or os.path.join(tests_directory, "keys")



@pytest.fixture()
def reset_environment():
    """
    Resets env variables.
    """
    init_env = os.environ.copy()
    yield
    changed_env = os.environ
    for key in changed_env:
        if key not in init_env:
            del os.environ[key]
        else:
            os.environ[key] = init_env[key]

@pytest.fixture()
def reset_db():
    """
    Resets Airflow db.
    """

    from airflow.utils import db

    db.resetdb()
    yield

def initial_db_init():
    from airflow.utils import db

    db.resetdb()
    db.bootstrap_dagbag()


@pytest.fixture(autouse=True, scope="session")
def initialize_airflow_tests(request):
    """
    Helper that setups Airflow testing environment.
    """

    print(" AIRFLOW ".center(60, "="))

    # Setup test environment for breeze
    home = os.path.expanduser("~")
    airflow_home = os.environ.get("AIRFLOW_HOME") or os.path.join(home, "airflow")

    print(f"Home of the user: {home}\nAirflow home {airflow_home}")

    # Initialize Airflow db if required
    lock_file = os.path.join(airflow_home, ".airflow_db_initialised")
    if request.config.option.db_init:
        print("Initializing the DB - forced with --with-db-init switch.")
        initial_db_init()
    elif not os.path.exists(lock_file):
        print(
            "Initializing the DB - first time after entering the container.\n"
            "You can force re-initialization the database by adding --with-db-init switch to run-tests."
        )
        initial_db_init()
        # Create pid file
        with open(lock_file, "w+"):
            pass
    else:
        print(
            "Skipping initializing of the DB as it was initialized already.\n"
            "You can re-initialize the database by adding --with-db-init flag when running tests."
        )


def pytest_addoption(parser):
    """
    Add options parser for custom plugins
    """
    group = parser.getgroup("airflow")
    group.addoption(
        "--with-db-init",
        action="store_true",
        dest="db_init",
        help="Forces database initialization before tests",
    )