import pytest

from conftest import CTS_URL, HTTPSTAT_US_URL, auth_user

from cdmtaskserviceclient.client import CTSClient, UnexpectedServerResponseError, InvalidTokenError


def test_constructor(auth_user):
    cli = CTSClient(f"   \t{auth_user[1]}   ", url=f"   {CTS_URL}\t   ")
    assert cli.user == auth_user[0]


def test_constructor_fail_no_token():
    for t in [None, "  \t  "]:
        with pytest.raises(ValueError) as e:
            CTSClient(t)
        assert str(e.value) == (
            "The token argument is required and cannot be a whitespace only string"
        )


def test_constructor_fail_no_url():
    for u in [None, "  \t  "]:
        with pytest.raises(ValueError) as e:
            CTSClient("token", url=u)
        assert str(e.value) == (
            "The url argument is required and cannot be a whitespace only string"
        )


def test_constructor_fail_error_5XX_response():
    with pytest.raises(UnexpectedServerResponseError) as e:
        CTSClient("token", url="https://ci.kbase.us/services/ws")
    assert str(e.value) == "Error response (500) from the CTS"


def test_constructor_fail_error_3XX_response():
    with pytest.raises(UnexpectedServerResponseError) as e:
        CTSClient("token", url=f"{HTTPSTAT_US_URL}/309")
    assert str(e.value) == ("Unexpected response (309) from the CTS")


def test_constructor_fail_error_4XX_response_not_json(auth_user):
    with pytest.raises(UnexpectedServerResponseError) as e:
        CTSClient("token", url="https://ci.kbase.us/services/foo")
    assert str(e.value) == "Unparseable error response (404) from the CTS"


def test_constructor_fail_error_unexpected_error_code(auth_user):
    with pytest.raises(UnexpectedServerResponseError) as e:
        # token gets checked before endpoint resolution
        CTSClient(auth_user[1], url=CTS_URL + "/fake")
    assert str(e.value) == (
        "Unexpected error code (40000) from the server: Not Found"
    )


def test_constructor_fail_bad_token():
    with pytest.raises(InvalidTokenError) as e:
        CTSClient("bad_token", url=CTS_URL)
    assert str(e.value) == "The authorization token is invalid"


def test_constructor_fail_success_response_not_json():
    with pytest.raises(UnexpectedServerResponseError) as e:
        CTSClient("token", url="https://example.com")
    assert str(e.value) == "Unparseable success response from the CTS"


def test_constructor_fail_service_not_cts():
    with pytest.raises(ValueError) as e:
        CTSClient("token", url="https://ci.kbase.us/services/groups")
    assert str(e.value) == (
        "The CTS url https://ci.kbase.us/services/groups "
        + "does not appear to point to the CTS service"
    )
