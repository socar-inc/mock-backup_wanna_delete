from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from rich import print
import re
import json
from google.cloud import secretmanager, bigquery
from typing import Any, Union, List


def get_table_info(table: str):
    client = get_bigquery_client()
    try:
        table = client.get_table(table=table)
        # print(f"Table [bold italic on white]{full_table_id}[/] [bold blue]does exists![/]")
        return table
    except NotFound as e:
        # print(f"Table [bold italic on white]{full_table_id}[/] [bold red]NOT found![/]")
        raise e
    except ValueError as e:
        raise e
    except Exception as e:
        raise e


def create_table_only_schema(source_table, target_table) -> str:
    client = get_bigquery_client()

    if target_table.split('.')[0] == "socar-data":
        print(f"Tables in socar-data project cannot be target table...!")
        raise Exception("Tables in socar-data project cannot be set as target table...!")

    sql = f"create or replace table {target_table} LIKE {source_table}"
    query_job = client.query_and_wait(query=sql)
    try:
        table_info = get_table_info(target_table)
        url = f"https://console.cloud.google.com/bigquery?project={table_info.project}&ws=!1m5!1m4!4m3!1s{table_info.project}!2s{table_info.dataset_id}!3s{table_info.table_id}"
        return url
    except NotFound as e:
        raise e
    except Exception as e:
        raise e


def validate_table_name(table):
    if not re.match("^[\w-]+(\.[\w-]+){2}$", table):
        raise ValueError(
            "Wrong input format! Please provide the proper format. ex) <project>.<dataset>.<table> ...")
    # elif not re.match("")
def convert_table_to_dev(table_list: Union[str, List[str]]):
    dev_table_list = list()
    for table in table_list:
        try:
            get_table_info(table)
            project, dataset, table = table.split('.')
            dev_table_list.append(f"socar-data-dev.{dataset}.{table}")
        except NotFound as e:
            raise e
        except Exception as e:
            raise e
    return dev_table_list

def _get_google_application_credentials(secret_name: str, project: str = 'prod', version: str = "latest") -> Any:
    client = secretmanager.SecretManagerServiceClient()
    project_id = '534391797303'
    if project == 'dev':
        project_id = '667575589991'
    secret = f"projects/{project_id}/secrets/{secret_name}"
    try:
        response = client.access_secret_version(request={"name": f"{secret}/versions/{version}"})
        payload = response.payload.data.decode("UTF-8")

        service_account_key_info = json.loads(payload)

        return service_account.Credentials.from_service_account_info(service_account_key_info)

    except Exception as e:
        print("Secret을 가져오는데 문제가 발생하였습니다.")
        raise e


def get_bigquery_client():
    try:
        credentials = _get_google_application_credentials('socar-data-mock')
        return bigquery.Client(credentials=credentials)
    except Exception as e:
        print(e)
