import copy


dq_config = {
    "username": "tech_admin",
    "product_name": "moz-b2c",  # namespace
    "src_type": "POSTGRESQL",
    "omd_service": "Postgresql",
    "src_db": "custosdq",
    "src_schema": "public",
    "src_table": "",
    "src_select_column": "",
    "src_condition": "",
    "src_conf": {
        "db_host": "217.25.88.97",
        "db_port": "5433"
    },
    "dq_conf": {},
    "spark_args": [],
    "subs_telegram_groups": [],
    "subs_email": [],
    "custos_api": "http://94.103.83.69:8004",
    "tocsin_api": ""
}


def get_dq_config():
    return copy.deepcopy(dq_config)
