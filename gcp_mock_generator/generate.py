
import typer, click
from rich import print
from gcp_mock_generator import utils
from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer
import numpy as np
app = typer.Typer()

#TODO 좀더 고민해봐야한다
@app.command()
def data(table: str):
    client = utils.get_bigquery_client()
    sql = f"SELECT * FROM {table} LIMIT 500"
    df = client.query(query=sql).to_dataframe(
        # int_dtype=None,
        # bool_dtype=None,
        # date_dtype=None,
        # time_dtype=None
    )
    # df = df.astype('object')

    number_column = df.select_dtypes(np.number)
    df[number_column.columns] = number_column.round().astype('float64')

    string_column = df.select_dtypes(object)

    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(data=df)
    print(string_column.columns)
    python_dict = metadata.to_dict()
    print(python_dict)
    for col in string_column.columns:
        metadata.update_column(
            column_name=col,
            sdtype='categorical'
        )
    python_dict = metadata.to_dict()
    print(python_dict)

    # df['zone_id'] = df['zone_id'].astype('Int64')
    # syn = SingleTablePreset(metadata, name="FAST_ML")
    # syn.fit(df)
    # data = syn.sample(num_rows=10)
    # print(data)
    # exit()
    # exit()
    syn = GaussianCopulaSynthesizer(
        metadata, # required
        enforce_min_max_values=True,
        enforce_rounding=True,
        # numerical_distributions={
        #     'zone_id': 'norm'
        # },
        # default_distribution='norm'
    )

    print(df.info())

    syn.fit(df)

    syn_data = syn.sample(num_rows=100)
    print(syn_data.info())
    exit()
    syn_data.to_gbq(
        project_id='socar-data-dev',
        destination_table='bbiyak_test.car_zone_day',
        if_exists='append',
        credentials=utils._get_google_application_credentials('socar-data-mock')
    )
    # try:
    #     table_info = utils.get_table_info(table)
    #     schema = table_info.schema
    # except NotFound as e:
    #     print(e)
    #     raise typer.Exit()
    # except ValueError as e:
    #     print(e)
    #     raise typer.Exit()
    # except Exception as e:
    #     print(e)
    #     raise typer.Exit()


if __name__ == "__main__":
    app()
