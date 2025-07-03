"""
A client for the CDM Task Service (CTS) and CDM Spark Event Processor (CSEP). 

Allows for submitting and checking the status of CTS jobs as well as subsequent processing steps
in the CSEP (if configured).
"""

import logging
import requests
from typing import Any


# TODO TEST logging


class CTSClient:
    """
    The main client class for the CDM Task Service.
    """
    
    def __init__(self, token: str, *, url: str = "https://ci.kbase.us/services/cts"):
        """
        Initialize the client.
        
        token - a KBase user token.
        url - the URL of the CTS.
        """
        self._headers = {"Authorization": f"Bearer {_require_string(token, 'token')}"}
        self._url = _require_string(url, "url")
        self._log = logging.getLogger(__name__)
        self._test_cts_connection()

    # The request methods are pretty similar to the CTS Event Processor repo code
    def _test_cts_connection(self):
        # Add retries here later if needed
        res = self._cts_request("", fail_on_500=True)
        if res.get("service_name") != "CDM Task Service":
            raise ValueError(
                f"The CTS url {self._url} does not appear to point to the CTS service"
            )
        # Test the token
        self.user = self._cts_request("whoami", fail_on_500=True)["user"]

    def _cts_request(
            self,
            url_path: str,
            body: dict[str, Any] = None,
            fail_on_500: bool = False,
    ) -> dict[str, Any]:
        # This fn will probably need changes as we discover error modes we've missed or
        # miscategorized as fatal or recoverable
        url = f"{self._url}/{url_path}"
        if body:
            res = requests.post(url, json=body, headers=self._headers)
        else:
            res = requests.get(url, headers=self._headers)
        if 400 <= res.status_code < 500:
            try:
                err = res.json()
            except Exception as e:
                self._log.exception(f"Unparseable error response from the CTS:\n{res.text}")
                raise UnexpectedServerResponseError(
                    f"Unparseable error response ({res.status_code}) from the CTS"
                ) from e
            if "error" not in err: # TODO TEST with mock, don't see a way to easily test o'wise
                self._log.error(f"Unexpected error structure from the CTS:\n{err}")
                raise UnexpectedServerResponseError(
                    f"Unexpected error structure, response ({res.status_code}) from the CTS"
                )
            errcode = err["error"].get("appcode")
            msg = err["error"].get("message")
            if errcode == 40040:
                raise NoSuchJobError(msg)
            if errcode == 10020:
                raise InvalidTokenError("The authorization token is invalid")
            raise UnexpectedServerResponseError(
                f"Unexpected error code ({errcode}) from the server: {msg}"
            )
        if res.status_code >= 500:
            # There's some 5XX errors that probably aren't recoverable but I've literally never
            # seem them in practice
            self._log.error(f"Error response from the CTS:\n{res.text}")
            errcls = UnexpectedServerResponseError if fail_on_500 else _PotentiallyRecoverableError
            raise errcls(f"Error response ({res.status_code}) from the CTS")
        if not (200 <= res.status_code < 300):
            # TODO TEST with httpstat.us down, can't find a site that returns a 1/3XX + text
            self._log.error(f"Unexpected response from the CTS:\n{res.text}")
            raise UnexpectedServerResponseError(
                f"Unexpected response ({res.status_code}) from the CTS"
            )
        try:
            return res.json()
        except Exception as e:
            self._log.exception(f"Unparseable response from the CTS:\n{res.text}")
            raise UnexpectedServerResponseError("Unparseable success response from the CTS") from e


def _require_string(putative: str, name: str) -> str:
    if not putative or not putative.strip():
        raise ValueError(f"The {name} argument is required and cannot be a whitespace only string")
    return putative.strip()


class InvalidTokenError(Exception):
    """ Thrown when the provided token is invalid. """


class NoSuchJobError(Exception):
    """ Thrown when the requested job does not exist. """


class UnexpectedServerResponseError(Exception):
    """ Thrown when the server returns an unexpected response. """


class _PotentiallyRecoverableError(Exception):  # used for retries
    pass
