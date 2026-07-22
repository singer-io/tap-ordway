from unittest import TestCase
from unittest.mock import MagicMock, patch
from datetime import datetime
from pytz import UTC
from requests.exceptions import ConnectionError as RequestsConnectionError
from tap_ordway.api.base import (
    InvalidCredentialsError,
    RequestHandler,
    _get_api_version,
    _get_headers,
    _get_url,
    validate_credentials,
)


@patch("tap_ordway.api.base.TAP_CONFIG")
def test_get_api_version(mocked_tap_config):
    mocked_tap_config.api_version = "v15"
    assert _get_api_version() == "v15"

    mocked_tap_config.api_version = "16"
    assert _get_api_version() == "v16"


@patch("tap_ordway.api.base.VERSION", "1.0.0")
@patch("tap_ordway.api.base.TAP_CONFIG")
def test_get_headers(mocked_tap_config):
    mocked_tap_config.api_credentials = {
        "company": "AmEx",
        "user_token": "123foo",
        "user_email": "foo@example.com",
        "api_key": "secret123",
    }
    expected_results = {
        "X-User-Company": "AmEx",
        "X-User-Token": "123foo",
        "X-User-Email": "foo@example.com",
        "X-API-KEY": "secret123",
        "User-Agent": "tap-ordway v1.0.0 (https://github.com/ordwaylabs/tap-ordway)",
        "Accept": "application/json",
    }

    assert _get_headers() == expected_results

    # Test with company_token
    mocked_tap_config.api_credentials["company_token"] = "company123"
    expected_results["X-Company-Token"] = "company123"

    assert _get_headers() == expected_results


class GetURLTestCase(TestCase):
    def setUp(self):
        self.tap_config_patcher = patch("tap_ordway.api.base.TAP_CONFIG")
        self.mocked_tap_config = self.tap_config_patcher.start()
        self.get_api_version_patcher = patch(
            "tap_ordway.api.base._get_api_version", return_value="v1"
        )
        self.mocked_get_api_version = self.get_api_version_patcher.start()

        self.mocked_tap_config.api_url = None
        self.mocked_tap_config.staging = False

    def tearDown(self):
        self.tap_config_patcher.stop()
        self.get_api_version_patcher.stop()

    def test_with_prod(self):
        expected_url = "https://api.ordwaylabs.com/api/v1/charges"

        self.assertEqual(_get_url("/charges"), expected_url)
        self.assertEqual(_get_url("charges"), expected_url)

    def test_with_staging(self):
        self.mocked_tap_config.staging = True

        self.assertEqual(
            _get_url("/charges"), "https://staging.ordwaylabs.com/api/v1/charges"
        )

    def test_with_configured_base_url(self):
        self.mocked_tap_config.api_url = "https://test.ordwaylabs.com/api/v22"
        expected_url = "https://test.ordwaylabs.com/api/v22/charges"

        self.assertEqual(_get_url("charges"), expected_url)
        self.assertEqual(_get_url("/charges"), expected_url)

        self.mocked_tap_config.api_url = "https://test.ordwaylabs.com/api/v22/"
        self.assertEqual(_get_url("/charges"), expected_url)


class RequestHandlerTestCase(TestCase):
    def setUp(self):
        self.get_patcher = patch(
            "tap_ordway.api.base.RequestHandler._get", autospec=True
        )
        self.mocked_get = self.get_patcher.start()
        self.request_handler = RequestHandler("/charges", page_size=45)

        self.mocked_data_context = MagicMock()
        self.mocked_data_context.parent_record = None

    def tearDown(self):
        self.get_patcher.stop()

    def test_resolve_endpoint_with_template_str(self):
        """Ensure resolve_endpoint is based off of context.parent_record,
        if parent_record is not None"""
        self.request_handler.endpoint_template = "/charges/{id}"
        self.mocked_data_context.parent_record = {"id": "CHG-99"}

        results = self.request_handler.resolve_endpoint(self.mocked_data_context)

        self.assertEqual(results, "/charges/CHG-99")

    def test_resolve_endpoint_with_missing_template_values_raises_exception(self):
        """If resolve_endpoint can't derive the template's values from
        parent_record, an exception should be thrown"""

        self.request_handler.endpoint_template = "/charges/{id}"
        self.mocked_data_context.parent_record = {"charge_id": "CHG-99"}

        with self.assertRaises(KeyError):
            self.request_handler.resolve_endpoint(self.mocked_data_context)

    def test_resolve_params_sort_and_filter_based_off_of_incremental_streams(self):
        """For streams that are configured as INCREMENTAl, we want to sort by the
        replication_key (updated_date) and filter thereby as well"""

        mocked_stream = MagicMock()
        mocked_stream.is_valid_incremental = True
        mocked_stream.replication_key = "updated_date"

        self.mocked_data_context.stream = mocked_stream
        self.mocked_data_context.filter_datetime = datetime(2020, 1, 1, tzinfo=UTC)

        self.assertDictEqual(
            self.request_handler.resolve_params(self.mocked_data_context),
            {"sort": "updated_date", "updated_date>": "2020-01-01T00:00:00.000000Z"},
        )

    def test_resolve_params_empty_if_stream_not_valid_incremental(self):
        mocked_stream = MagicMock()
        mocked_stream.is_valid_incremental = False

        self.mocked_data_context.stream = mocked_stream

        self.assertDictEqual(
            self.request_handler.resolve_params(self.mocked_data_context),
            {},
        )

    def test_resolve_params_defaults_to_request_handler_sort(self):
        """If RequestHandler.sort is defined, resolve_params should not
        set the sort key"""

        self.request_handler.sort = "id"

        mocked_stream = MagicMock()
        mocked_stream.is_valid_incremental = True
        mocked_stream.replication_key = "updated_date"

        self.mocked_data_context.stream = mocked_stream
        self.mocked_data_context.filter_datetime = datetime(2020, 1, 1, tzinfo=UTC)

        self.assertDictEqual(
            self.request_handler.resolve_params(self.mocked_data_context),
            {"updated_date>": "2020-01-01T00:00:00.000000Z"},
        )

    def test_fetch_invokes_get(self):
        self.mocked_get.return_value = []

        with patch.object(self.request_handler, "resolve_params", return_value={}):
            list(self.request_handler.fetch(self.mocked_data_context))

            self.mocked_get.assert_called_once_with(
                self.request_handler, "/charges", {"sort": None, "size": 45, "page": 1}
            )


class ValidateCredentialsTestCase(TestCase):
    def setUp(self):
        self.session_patcher = patch("tap_ordway.api.base.Session")
        self.mocked_session_cls = self.session_patcher.start()
        self.mocked_session = self.mocked_session_cls.return_value
        self.mocked_response = MagicMock()
        self.mocked_session.get.return_value = self.mocked_response

        self.tap_config_patcher = patch("tap_ordway.api.base.TAP_CONFIG")
        self.mocked_tap_config = self.tap_config_patcher.start()
        self.mocked_tap_config.api_url = None
        self.mocked_tap_config.staging = False
        self.mocked_tap_config.api_version = "v1"
        self.mocked_tap_config.api_credentials = {
            "company": "TestCo",
            "user_token": "tok",
            "user_email": "test@example.com",
            "api_key": "key",
        }

    def tearDown(self):
        self.session_patcher.stop()
        self.tap_config_patcher.stop()

    def test_succeeds_on_200(self):
        """A 200 response means credentials are valid — no exception raised."""
        self.mocked_response.status_code = 200
        self.mocked_response.ok = True

        validate_credentials()  # should not raise

        self.mocked_session.get.assert_called_once()
        _, kwargs = self.mocked_session.get.call_args
        self.assertEqual(kwargs["params"], {"size": 1, "page": 1})

    def test_raises_invalid_credentials_on_401(self):
        """HTTP 401 must raise InvalidCredentialsError."""
        self.mocked_response.status_code = 401
        self.mocked_response.ok = False

        with self.assertRaises(InvalidCredentialsError):
            validate_credentials()

    def test_raises_invalid_credentials_on_403(self):
        """HTTP 403 must raise InvalidCredentialsError."""
        self.mocked_response.status_code = 403
        self.mocked_response.ok = False

        with self.assertRaises(InvalidCredentialsError):
            validate_credentials()

    def test_raises_http_error_on_other_non_2xx(self):
        """Non-auth HTTP errors (e.g. 500) should surface via raise_for_status."""
        from requests.exceptions import HTTPError
        self.mocked_response.status_code = 500
        self.mocked_response.ok = False
        self.mocked_response.text = "Internal Server Error"
        self.mocked_response.raise_for_status.side_effect = HTTPError("500 Server Error")

        with self.assertRaises(HTTPError):
            validate_credentials()

        self.mocked_response.raise_for_status.assert_called_once()

    def test_propagates_request_exception_on_network_error(self):
        """Network-level errors must propagate as-is."""
        self.mocked_session.get.side_effect = RequestsConnectionError("timeout")

        with self.assertRaises(RequestsConnectionError):
            validate_credentials()

    def test_error_message_contains_status_code(self):
        """InvalidCredentialsError message should mention the HTTP status code."""
        self.mocked_response.status_code = 401
        self.mocked_response.ok = False

        with self.assertRaises(InvalidCredentialsError) as ctx:
            validate_credentials()

        self.assertIn("401", str(ctx.exception))
