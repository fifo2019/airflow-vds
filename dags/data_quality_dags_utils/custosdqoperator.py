import requests
import time
import json
import uuid
from typing import Optional, List, Dict
from datetime import datetime, timedelta

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowFailException
# from airflow.contrib.secrets.hashicorp_vault import VaultBackend
from airflow.models import Variable

from jinja2 import Template, Environment, meta

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def send_to_tocsin_status_dq(tocsin_api: str,
                             status: int,
                             fqn: str,
                             subs_telegram_groups: Optional[List] = None,
                             subs_emails: Optional[List] = None):
    """
    Send status to tocsin api.

    :param tocsin_api: URL tocsin api.
    :type tocsin_api: str
    :param status: Status data quality (1, 0, -1).
    :type status: int
    :param fqn: Fully qualified name
    :type fqn: str
    :param subs_telegram_groups: List of subscribe telegram groups.
    :type subs_telegram_groups: Optional[List]
    :param subs_emails: List of subscribe.
    :type subs_emails: Optional[List]
    """
    print(f"Info send to tocsin. Status: {status}, fqn: {fqn}, subs_telegram_groups: {subs_telegram_groups}, subs_emails: {subs_emails}.")
    # r = requests.post(
    #     f"{tocsin_api}/custosDQ",
    #     data=json.dumps({
    #         "status": status,  # 1, 0, -1
    #         "fqn": fqn,
    #         "subs_telegram_groups": subs_telegram_groups,
    #         "subs_emails": subs_emails
    #     }),
    #     headers={'Content-Type': 'application/json'},
    #     verify=False
    # )
    #
    # if r.status_code in (200, 201, 204):
    #     print(f"\nInfo send to tocsin. Status code: {r.status_code}. Date: {r.headers['Date']}\n")
    # else:
    #     print(f'\nNo info send to tocsin. Status code: {r.status_code}')


def send_to_tocsin_error_msg(tocsin_api: str,
                             public_args: Dict,
                             traceback: str):
    """
    Send text of error to tocsin api.

    :param tocsin_api: URL tocsin api.
    :type tocsin_api: str
    :param public_args: Configuration dict to run dq app (public).
    :type public_args: str
    :param traceback: Text of error to send custosdq support.
    :type traceback: str
    """
    print(f"\nInfo send to tocsin. Status: {public_args}, traceback: {traceback}.")
    # r = requests.post(
    #     f"{tocsin_api}/traceback",
    #     data=json.dumps({
    #         "public_args": public_args,
    #         "traceback": traceback,
    #     }),
    #     headers={'Content-Type': 'application/json'},
    #     verify=False
    # )
    #
    # if r.status_code in (200, 201, 204):
    #     print(f"\nInfo send to tocsin. Status code: {r.status_code}. Date: {r.headers['Date']}\n")
    # else:
    #     print(f'\nNo info send to tocsin. Status code: {r.status_code}')


def dq_status(context: Dict):
    """
    Prepare data to send status to tocsin api. Like 1, 0 or -1

    :param context: Configuration dict to run dq app.
    :type context: Dict
    """
    send_to_tocsin_status_dq(
        tocsin_api=context.get("tocsin_api"),
        status=context.get("status_dq"),
        fqn=context.get("fqn"),
        subs_telegram_groups=context.get("subs_telegram_groups"),
        subs_emails=context.get("subs_emails")
    )


class CustosDQOperator(BaseOperator):
    """
    Runs a data quality check on the data. Prepare data and send configuration on spark app to custosdq api.

    :param dq_config: Configuration dict to run dq app.
    :type dq_config: Dict
    :param poke_interval: Interval between application status requests (in seconds, default 20).
    :type poke_interval: int
    :param timeout: The lifetime of the application, after expiration, the task will fail (in seconds, default 1800).
    :type timeout: int
    :param max_attempt: The number of max attempt that should be performed before failing the task (default 2).
    :type max_attempt: int
    :param retry_delay_task: delay between attempt (in seconds, default 60).
    :type retry_delay_task: int
    """

    @apply_defaults
    def __init__(self,
                 dq_config: dict,
                 poke_interval: int = 20,
                 timeout: int = 1800,
                 max_attempt: int = 2,
                 retry_delay_task: int = 60,
                 *args,
                 **kwargs):

        if not dq_config:
            raise AirflowFailException("dq_config is empty or not found")

        self.dq_config = dq_config
        self.spark_app_name = self.get_spark_app()
        self.fqn = f"{dq_config['omd_service']}.{dq_config['src_db']}.{dq_config['src_schema']}.{dq_config['src_table']}"
        super().__init__(task_id=f'{self.spark_app_name}', *args, **kwargs)

        self.jinja_env = Environment()

        self.custos_api = self.dq_config.pop('custos_api')
        self.tocsin_api = self.dq_config.pop('tocsin_api')

        self.post_url = f"{self.custos_api}/api/v1/dq-task"
        self.get_url = f"{self.custos_api}/api/v1/dq-task/status"

        self.poke_interval = poke_interval
        self.timeout = timeout
        self.max_attempt = max_attempt
        self.retry_delay_task = retry_delay_task

        self.on_success_callback = dq_status
        self.on_failure_callback = dq_status

        self.dq_status_success = 1
        self.dq_status_failure = 0
        self.task_status_failure = -1

        self.url = None
        self.kv_engine_version = 1
        self.mount_point = None
        self.auth_type = None
        self.token = None
        self.kubernetes_role = None
        self.secret_path_prefix = "custosdq"
        self.secret_name = "config"

    @staticmethod
    def get_fail_message(**kwargs):
        return ", ".join([f"{key}: {value}" for key, value in kwargs.items()])

    # def _set_variables(self):
    #     self.url = self._get_value(self.url, 'vault_url')
    #     self.mount_point = self._get_value(self.mount_point, 'vault_mount_point')
    #     self.auth_type = self._get_value(self.auth_type, 'vault_auth_type')
    #     self.token = self._get_value(self.token, 'vault_token')
    #     self.kubernetes_role = self._get_value(self.kubernetes_role, 'vault_kubernetes_role')

    @staticmethod
    def _get_value(value, variable_key) -> str:
        return Variable.get(variable_key, None) if value is None else value

    # def _get_backend(self) -> Variable:
    #     # self._set_variables()
    #     return Variable(url=self.url, verify=False, mount_point=self.mount_point, auth_type=self.auth_type,
    #                         kv_engine_version=self.kv_engine_version, token=self.token,
    #                         kubernetes_role=self.kubernetes_role)

    def get_spark_app(self):

        src_schema = self.dq_config.get("src_schema") if self.dq_config.get("src_schema") else ""
        src_table = self.dq_config.get("src_table") if self.dq_config.get("src_table") else ""

        if src_schema or src_table:
            src_conf = self.dq_config.get('src_conf')
            dq_task_id_postfix = ""
            if src_conf:
                extra_name = src_conf.get('extra_name')
                dq_task_id_postfix = f"__{extra_name}" if extra_name else ""
            return f"dq__{src_schema}__{src_table}{dq_task_id_postfix}"

        return f"dq__{uuid.uuid4()}"

    def execute(self, context):

        context["fqn"] = self.fqn
        context["subs_telegram_groups"] = self.dq_config.get("subs_telegram_groups")
        context["subs_emails"] = self.dq_config.get("subs_email")
        context["tocsin_api"] = self.tocsin_api
        context["status_dq"] = None

        attempt = 1

        while attempt <= self.max_attempt:

            print(f"Attempt: {attempt}. Max attempt: {self.max_attempt} - Executing task...")

            try:

                # secrets = self._get_backend()._get_secret(
                #     path_prefix=self.secret_path_prefix, secret_id=self.secret_name)

                self.dq_config["dq_token"] = Variable.get("DQ_TOKEN")
                self.dq_config["spark_app_name"] = self.spark_app_name
                self.dq_config["task_source"] = "airflow"
                self.dq_config["task_source_detail"] = context.get("task_instance_key_str") \
                    if context.get("task_instance_key_str") else ""
                self.dq_config["execution_datetime_utc"] = context.get("execution_date").in_tz('UTC').int_timestamp
                self.dq_config["execution_date"] = context.get("execution_date").strftime('%Y-%m-%d')

                src_condition = self.dq_config.get('src_condition')
                if src_condition:
                    self.dq_config["src_condition"] = self.jinja_env.from_string(src_condition).render(context)

                start_time = datetime.now()
                timeout_time = start_time + timedelta(seconds=self.timeout)

                print(f"start_time: {start_time}")
                print(f"timeout_time: {timeout_time}")

                response_create = requests.post(self.post_url, json.dumps(self.dq_config), verify=False)
                if response_create.status_code == 201:
                    result_create = response_create.json()
                    task_id = result_create.get("task_id")

                    if task_id:
                        response_status = requests.get(f"{self.get_url}/{task_id}", verify=False)
                        if response_status.status_code in (200, 201):

                            print(response_status.json())
                            result_status = response_status.json()

                            while result_status['task_status'] != 'SUCCESS':

                                if datetime.now() > timeout_time:
                                    raise AirflowFailException(
                                        self.get_fail_message(
                                            error_message="Timeout reached for the task.",
                                            task_id=result_status["task_id"],
                                            task_status=result_status["task_status"],
                                            recommendation="PLEASE RELOAD THE DQ TASK!"
                                        )
                                    )

                                print(f"Status: {result_status['task_status']}")

                                time.sleep(self.poke_interval)

                                response_status = requests.get(f"{self.get_url}/{task_id}", verify=False)
                                result_status = response_status.json()

                                if result_status['task_status'] in ("FAILURE", "UNKNOWN"):
                                    raise AirflowFailException(
                                        self.get_fail_message(
                                            task_id=result_status["task_id"],
                                            task_status=result_status["task_status"],
                                            recommendation="PLEASE CHECK YOUR APP IN YARN AND RELOAD THE DQ TASK!"
                                        )
                                    )

                            print(f"Status: {result_status['task_status']}")

                            if int(result_status["task_result"]) == self.dq_status_success:
                                context["status_dq"] = self.dq_status_success
                                print("Completed dq task!")
                                break
                            elif int(result_status["task_result"]) == self.dq_status_failure:
                                context["status_dq"] = self.dq_status_failure
                                raise AirflowFailException(
                                    self.get_fail_message(
                                        error_message="DATA HAS NOT PASSED DATA QUALITY CHECK!",
                                        task_id=result_status["task_id"],
                                        DQ_STATUS="FAILED",
                                        DQ_RESULT=self.dq_status_failure,
                                        recommendation="PLEASE CHECK DQ RESULT IN OMD AND YOUR APP!"
                                    )
                                )
                            else:
                                raise AirflowFailException(
                                    self.get_fail_message(
                                        error_message="Don't know status task!",
                                        task_id=result_status["task_id"],
                                        task_status=result_status["task_status"],
                                        task_result=result_status["task_result"],
                                        recommendation="PLEASE SEND THIS MESSAGE TO SUPPORT OF CUSTOS!"
                                    )
                                )
                        else:
                            raise AirflowFailException(
                                self.get_fail_message(
                                    error_message="Don't get status task!",
                                    status_code=response_status.status_code,
                                    response=response_status.json(),
                                    recommendation="PLEASE RELOAD THE DQ TASK AFTER 5 MINUTES!"
                                )
                            )
                    else:
                        raise AirflowFailException(
                            self.get_fail_message(
                                error_message=f"Don't know task for task id: {task_id}",
                                recommendation="PLEASE RELOAD THE DQ TASK AFTER 5 MINUTES!"
                            )
                        )
                else:
                    raise AirflowFailException(
                        self.get_fail_message(
                            error_message="Don't create dq task!",
                            status_code=response_create.status_code,
                            response=response_create.json(),
                            recommendation="PLEASE RELOAD THE DQ TASK AFTER 5 MINUTES!"
                        )
                    )

            except Exception as e:

                import traceback

                if context.get("status_dq") is None:
                    context["status_dq"] = self.task_status_failure

                dq_token = self.dq_config.get("dq_token")
                self.dq_config["dq_token"] = f"{dq_token[:4]}***********" if dq_token else None
                self.dq_config["status_dq"] = context.get("status_dq")

                send_to_tocsin_error_msg(
                    self.tocsin_api,
                    self.dq_config,
                    f"Attempt: {attempt}. Max attempt: {self.max_attempt}. {traceback.format_exc()}"
                )

                print(f"Attempt: {attempt}. Max attempt: {self.max_attempt} - Failed with: {str(e)}")

                if context.get("status_dq") == self.dq_status_failure:
                    raise AirflowFailException(e)

                if attempt < self.max_attempt:
                    print(f"Retry delay: {self.retry_delay_task} seconds.")
                    time.sleep(self.retry_delay_task)
                    print("Retrying task...")
                    context["status_dq"] = None
                    self.dq_config.pop("status_dq")
                else:
                    raise AirflowFailException(e)

            attempt += 1
