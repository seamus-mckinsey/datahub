from unittest.mock import MagicMock, patch

import pydantic
import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.metabase import (
    MetabaseConfig,
    MetabaseReport,
    MetabaseSource,
)


class TestMetabaseSource(MetabaseSource):
    def __init__(self, ctx: PipelineContext, config: MetabaseConfig):
        self.config = config
        self.report = MetabaseReport()


def test_get_platform_instance():
    ctx = PipelineContext(run_id="test-metabase")
    config = MetabaseConfig()
    config.connect_uri = "http://localhost:3000"
    # config.database_id_to_instance_map = {"42": "my_main_clickhouse"}
    # config.platform_instance_map = {"clickhouse": "my_only_clickhouse"}
    metabase = TestMetabaseSource(ctx, config)

    # no mappings defined
    assert metabase.get_platform_instance("clickhouse", 42) is None

    # database_id_to_instance_map is defined, key is present
    metabase.config.database_id_to_instance_map = {"42": "my_main_clickhouse"}
    assert metabase.get_platform_instance(None, 42) == "my_main_clickhouse"

    # database_id_to_instance_map is defined, key is missing
    assert metabase.get_platform_instance(None, 999) is None

    # database_id_to_instance_map is defined, key is missing, platform_instance_map is defined and key present
    metabase.config.platform_instance_map = {"clickhouse": "my_only_clickhouse"}
    assert metabase.get_platform_instance("clickhouse", 999) == "my_only_clickhouse"

    # database_id_to_instance_map is defined, key is missing, platform_instance_map is defined and key missing
    assert metabase.get_platform_instance("missing-platform", 999) is None

    # database_id_to_instance_map is missing, platform_instance_map is defined and key present
    metabase.config.database_id_to_instance_map = None
    assert metabase.get_platform_instance("clickhouse", 999) == "my_only_clickhouse"

    # database_id_to_instance_map is missing, platform_instance_map is defined and key missing
    assert metabase.get_platform_instance("missing-platform", 999) is None


def test_set_display_uri():
    display_uri = "some_host:1234"

    config = MetabaseConfig.model_validate({"display_uri": display_uri})

    assert config.connect_uri == "localhost:3000"
    assert config.display_uri == display_uri


@patch("requests.session")
def test_connection_uses_api_key_if_in_config(mock_session):
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000", api_key=pydantic.SecretStr("key")
    )
    ctx = PipelineContext(run_id="metabase-test-apikey")

    mock_session_instance = MagicMock()
    mock_session_instance.headers = {}
    mock_session.return_value = mock_session_instance

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_session_instance.get.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.close()

    mock_session_instance.get.assert_called_once_with("localhost:3000/api/user/current")
    request_headers = mock_session_instance.headers
    assert request_headers["x-api-key"] == "key"


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_create_session_from_config_username_password(mock_post, mock_get, mock_delete):
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000", username="un", password=pydantic.SecretStr("pwd")
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.close()

    kwargs_post = mock_post.call_args
    assert kwargs_post[0][0] == "localhost:3000/api/session"
    assert kwargs_post[0][2]["password"] == "pwd"
    assert kwargs_post[0][2]["username"] == "un"

    kwargs_get = mock_get.call_args
    assert kwargs_get[0][0] == "localhost:3000/api/user/current"

    mock_delete.assert_called_once()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_fail_session_delete(mock_post, mock_get, mock_delete):
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000", username="un", password=pydantic.SecretStr("pwd")
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response

    mock_response_delete = MagicMock()
    mock_response_delete.status_code = 400
    mock_delete.return_value = mock_response_delete

    mock_report = MagicMock()

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.report = mock_report
    metabase_source.close()

    mock_report.report_failure.assert_called_once()


class StubMetabaseSource(MetabaseSource):
    def __init__(
        self,
        ctx: PipelineContext,
        config: MetabaseConfig,
        datasource_tuple,
        source_table_tuple,
    ):
        self.config = config
        self.report = MetabaseReport()
        self._datasource_tuple = datasource_tuple
        self._source_table_tuple = source_table_tuple

    def get_datasource_from_id(self, datasource_id):  # type: ignore[override]
        return self._datasource_tuple

    def get_source_table_from_id(self, table_id):  # type: ignore[override]
        return self._source_table_tuple


@pytest.mark.parametrize(
    "schema_name,table_name,expected_table",
    [
        ("dw-color", "marts.care_program_participants", "marts.care_program_participants"),
        (
            "dw-color",
            "dw-color.marts.care_program_participants",
            "marts.care_program_participants",
        ),
        (
            "dw-color",
            "region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT",
            "region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT",
        ),
    ],
)
def test_mbql_bigquery_table_name_parsing(schema_name, table_name, expected_table):
    ctx = PipelineContext(run_id="test-metabase-bq-mbql")
    config = MetabaseConfig()

    source = StubMetabaseSource(
        ctx,
        config,
        datasource_tuple=("bigquery", "dw-color", None, "dw-color"),
        source_table_tuple=(schema_name, table_name),
    )

    card_details = {
        "database_id": 1,
        "dataset_query": {"type": "query", "query": {"source-table": 123}},
    }

    result = source.get_datasource_urn(card_details)
    assert result is not None

    expected_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,"
        f"dw-color.{expected_table},{config.env})"
    )
    assert result[0] == expected_urn
