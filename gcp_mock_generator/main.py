import typer
from typing import List
from typing_extensions import Annotated
from rich import print
from rich.prompt import Prompt, Confirm
from rich.table import Table
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from pathlib import Path

from gcp_mock_generator import utils

app = typer.Typer()


@app.command()
def mock(
        source_table_list: Annotated[
            List[str],
            typer.Argument(
                help="List of tables separated by space you want to mock",
                rich_help_panel="Customization and Utils"
            )
        ],
):
    """
    Mock tables to GCP dev project with empty data
    :param source_table_list:
    :return: result of mocked tables
    """
    # source_table_list 가 <project>.<dataset>.<table> 형식으로 들어왔는지 테스트 필요
    for source_table in source_table_list:
        try:
            utils.validate_table_name(source_table)
        except ValueError as e:
            print(e)
            raise typer.Exit(code=422)
        except Exception as e:
            print(e)
            raise typer.Exit(code=400)

    # target_table 의 project 는 socar-data-dev 로 고정
    try:
        target_table_list = utils.convert_table_to_dev(source_table_list)
    except NotFound as e:
        print(e)
        raise typer.Exit(code=404)
    except Exception as e:
        print(e)
        raise typer.Exit(code=400)

    for source_table, target_table in zip(source_table_list, target_table_list):
        try:
            utils.get_table_info(target_table)
            if Confirm.ask(
                    prompt=f"{target_table} [bold red]already exists![/] Do you want to [bold red]overwrite?[/]",
                    default=True):
                url = utils.create_table_only_schema(source_table=source_table, target_table=target_table)
                print(f"{target_table} [bold green]successfully created![/] {url}")
            else:
                print("Skip overwriting table ...")
                continue

        except NotFound:
            if Confirm.ask(prompt=f"{target_table} [bold red]does not exists![/] Do you want to [bold red]create?[/]",
                           default=True):
                url = utils.create_table_only_schema(source_table=source_table, target_table=target_table)
                print(f"{target_table} [bold green]successfully created![/] {url}")
            else:
                print("Skip creating table ...")
                continue

        except Exception as e:
            print(e)
            typer.Exit(code=400)


@app.command()
def check():
    """
    Check if the interactively specified table exists or not and print the metadata
    """
    project_list = ["socar-data", "socar-data-dev", "socar-data-internal"]
    project = Prompt.ask(prompt="Enter the Project:", choices=project_list, default=project_list[0])
    dataset = Prompt.ask(prompt="Enter the Dataset", default="tianjin_replica")
    table = Prompt.ask(prompt="Enter the Table", default="reservation_info")
    table_id = f"{project}.{dataset}.{table}"
    print(f"Your table_id: [bold italic on white]{table_id}[/]")
    try:
        table_info = utils.get_table_info(table_id)
    except NotFound as e:
        print(e)
        raise typer.Exit()
    except Exception as e:
        print(e)
        raise typer.Exit()
    table = Table(show_header=True, header_style="bold blue")
    table.add_column("name")
    table.add_column("type")
    table.add_column("mode")
    table.add_column("description")

    for t in table_info.schema:
        table.add_row(t.name, t.field_type, t.mode, t.description)
    print(table)
    print(f"{table_id} has [blue]{table_info.num_rows:,}[/] rows")


@app.command()
def parse(sql_path: Path):
    """
    Parse the BigQuery sql provided in sql_path and mock the referenced tables
    :param sql_path:
    :return:
    """
    if sql_path.is_file():
        print(f"This file {sql_path.name} [bold green]exists![/]")
        try:
            with open(sql_path, 'r') as f:
                sql = f.read()
                client = utils.get_bigquery_client()
                job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
                query_job = client.query(query=sql, job_config=job_config)
                query_job.result()
                referenced_tables = [f"{ref.project}.{ref.dataset_id}.{ref.table_id}" for ref in
                                     query_job.referenced_tables]
                print(f"Referenced tables: {referenced_tables}")
                dev_table_list = utils.convert_table_to_dev(referenced_tables)
                mock(dev_table_list)

        except Exception as e:
            print(e)
            raise typer.Exit(400)
    else:
        print(f"File not found")
        raise typer.Exit(404)


if __name__ == "__main__":
    app()
