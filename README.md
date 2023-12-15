# gcp_mock_generator
- GCP Prod 에 존재하는 테이블을 GCP Dev 에 빠르게 생성하고 싶을 때!
- GCP Prod에 존재하는 테이블과 동일한 메타데이터를 갖는 테이블을 socar-data-dev에 생성할 수 있습니다

## Installation
```shell
pip install gcp-mock-generator
```

```Usage
> gcp-mock-generator --help

Usage: gcp-mock-generator [OPTIONS] COMMAND [ARGS]...                                                                                                                                                                
                                                                                                                                                                                                                      
╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --install-completion          Install completion for the current shell.                                                                                                                                            │
│ --show-completion             Show completion for the current shell, to copy it or customize the installation.                                                                                                     │
│ --help                        Show this message and exit.                                                                                                                                                          │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ check        Check if the interactively specified table exists or not and print the metadata                                                                                                                       │
│ mock         Mock tables to GCP dev project with empty data :param source_table_list: :return: result of mocked tables                                                                                             │
│ parse        Parse the BigQuery sql provided in sql_path and mock the referenced tables :param sql_path: :return:                                                                                                  │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```