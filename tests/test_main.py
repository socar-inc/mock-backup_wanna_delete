from typer.testing import CliRunner

from gcp_mock_generator.main import app

runner = CliRunner()


def test_mock_happy_path():
    result = runner.invoke(app, ["mock", "socar-data.bbiyak_test.car_zone_day"], input="y")
    assert result.exit_code == 0  # Good
    assert "exists!" in result.stdout
    assert "successfully created!" in result.stdout


def test_mock_table_not_exist():
    result = runner.invoke(app, ["mock", "socar-data.bbiyak_test.car_zone_day2"])
    assert result.exit_code == 404  # Not Found
    assert "Not found" in result.stdout


def test_mock_wrong_input():
    result = runner.invoke(app, ["mock", "bbiyak_test.car_zone_day"])
    assert result.exit_code == 422  # InvalidArgumentException
    assert "Wrong input format" in result.stdout


def test_parse_file_not_exist():
    result = runner.invoke(app, ["parse", "./sdsd.sql"])
    assert result.exit_code == 404
    assert "File not found" in result.stdout