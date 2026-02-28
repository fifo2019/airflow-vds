
from sqlalchemy import create_engine
from airflow.models import Variable

def get_psql_engine():
    psql_user = Variable.get("psql_user")
    psql_password = Variable.get("psql_password")
    psql_host = Variable.get("psql_host")
    psql_port = Variable.get("psql_port")
    psql_db = Variable.get("psql_db")
    return create_engine(f'postgresql://{psql_user}:{psql_password}@{psql_host}:{psql_port}/{psql_db}')