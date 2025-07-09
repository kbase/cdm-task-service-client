"""
A client for the CDM Task Service (CTS) and CDM Spark Event Processor (CSEP). 

Allows for submitting and checking the status of CTS jobs as well as subsequent processing steps
in the CSEP (if configured).
"""

import json
import logging
import requests
import time
from typing import Any, Self


# TODO TEST logging


_EVENT_COMPLETION_KEYS = {"cse_event_processing_complete", "cse_event_processing_error"}


class InsertFiles:
    """
    Insert a list of files at the specified location in the argument list.
    
    Files will be automatically split evenly between Docker containers if more than one
    container is specified.
    """
    
    MODE_SPACE_SEPARATED = "space_separated_list"
    """ Insert the files as a space separated list """
    
    MODE_COMMA_SEPARATED = "comma_separated_list"
    """ Insert the files as a comma separated list """
    
    # TODO FUTURE support repeat parameter arguments when needed, seems like an unusual feature

    @classmethod
    def spacesep(cls) -> Self:
        """ Insert a space separated list of files. """
        inst = cls()
        inst._mode = cls.MODE_SPACE_SEPARATED
        return inst
    
    @classmethod
    def commasep(cls) -> Self:
        """ Insert a comma separated list of files. """
        inst = cls()
        inst._mode = cls.MODE_COMMA_SEPARATED
        return inst
    
    def render(self) -> dict[str, str]:
        """ Renders the InsertFiles directive into the format understood by the CTS. """
        return {
            "type": "input_files",
            "input_files_format": self._mode,
        }


class InsertContainerNumber:
    """ Insert the container number at the specified location in the argument list. """
    
    def __init__(self, *, prefix: str | None = None, suffix: str | None = None):
        """
        Create the insert container number directive.
        
        prefix - an optional prefix to prepend to the container number.
        suffix - an optional suffix to append to the container number.
        """
        self._prefix = prefix.strip() if prefix and prefix.strip() else None
        self._suffix = suffix.strip() if suffix and suffix.strip() else None
    
    def render(self) -> dict[str, str]:
        """ Renders the InsertContainerNumber directive into the format understood by the CTS. """
        return {
            "type": "container_number",
            "container_num_prefix": self._prefix,
            "container_num_suffix": self._suffix,
        }


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
        res = self._cts_request("")
        if res.get("service_name") != "CDM Task Service":
            raise ValueError(
                f"The CTS url {self._url} does not appear to point to the CTS service"
            )
        # Test the token
        self.user = self._cts_request("whoami")["user"]

    def _raise_err(self, err: dict[str, Any]):
        # maybe should just returnthe entire error dict in exceptions
        errcode = err["error"].get("appcode")
        msg = err["error"].get("message")
        self._log.error(f"CTS returned error structure:\n{err}")
        if errcode == 10020:
            raise InvalidTokenError("The authorization token is invalid")
        if errcode == 20000:
            raise UnauthorizedError(msg)
        if errcode == 30010:
            raise SubmissionStructureError(
                "The CDM Task Service rejected the job submission request",
                err["error"]["request_validation_detail"]
            )
        if errcode == 40040:
            raise NoSuchJobError(msg)
        if errcode in (
            # doesn't seem like we need specific error classes for these, they all mean no
            # job submission for you
            20010,  # S3 path inaccessible
            20020,  # S3 bucket inaccessible
            30001,  # Illegal parameter
            30040,  # Illegal image name
            40010,  # S3 path not found
            40030,  # No such image
            60000,  # Job flow unavailable 
        ):
            raise SubmissionError(msg)
        # fallback
        raise UnexpectedServerResponseError(
            f"Unexpected error code ({errcode}) from the server: {msg}"
        )

    def _cts_request(
            self,
            url_path: str,
            body: dict[str, Any] | None = None,
            fail_on_500: bool = True,
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
            self._raise_err(err)
        if res.status_code >= 500:
            # There's some 5XX errors that probably aren't recoverable but I've literally never
            # seem them in practice
            self._log.error(f"Error response from the CTS:\n{res.text}")
            errcls = UnexpectedServerResponseError if fail_on_500 else _PotentiallyRecoverableError
            raise errcls(f"Error response ({res.status_code}) from the CTS")
        if not (200 <= res.status_code < 300):
            self._log.error(f"Unexpected response from the CTS:\n{res.text}")
            raise UnexpectedServerResponseError(
                f"Unexpected response ({res.status_code}) from the CTS"
            )
        try:
            return res.json()
        except Exception as e:
            self._log.exception(f"Unparseable response from the CTS:\n{res.text}")
            raise UnexpectedServerResponseError("Unparseable success response from the CTS") from e

    def get_job_by_id(self, job_id: str) -> "Job":  # yuck, but this is the least bad sol'n
        """
        Get a Job instance given a job ID. The instance is lazily created - the existence
        of the job is not checked until a method is called that contacts the service.
        """
        return Job(_require_string(job_id, "job_id"), self)
    
    def submit_job(
        self,
        image: str,
        # TODO FUTURE support data IDs and crc64nvmes when needed
        input_files: list[str],
        output_dir: str,
        *,
        cluster: str = "perlmutter-jaws",
        input_mount_point: str | None = None,
        output_mount_point: str | None = None,
        refdata_mount_point: str | None = None,
        # TODO FUTURE support manifest files when needed, seems like an unusual feature
        args: list[str | InsertFiles | InsertContainerNumber] | None = None,
        num_containers: int = 1,
        cpus: int = 1,
        memory: int | str = "10MB",
        runtime: int | str = "PT5M",
        log_body: bool = False,
    ) -> "Job":
        """
        Submit  a job request to the service.
        
        WARNING - the resource requirement defaults are very low. Please inspect them carefully
        and adjust to fit the needs of your job.
        
        image - the Docker image to run. The image must be registered in the CTS. 
            Images are listable at the images endpoint in the service.
        input_files - a list of S3 / Minio files that will be processed as part of the job.
            WARNING: whitespace characters are valid in S3 key names and are *not* stripped
            from any input strings.
            The files must start with the bucket, e.g. `<bucket>/<key>`.
            The files will be mounted into the Docker container(s) at the `input_mount_point.
            Any path information other than the file name is discarded.
        output_dir - a S3 / Minio path where the files should be saved. Must start with the
            bucket.
        cluster - the compute cluster where the job should run. Currently the only option is
            perlmutter-jaws.
        input_mount_point - where the input files should be mounted in the Docker container.
            Must start from the container root and include at least one directory when resolved.
            The CTS default is /input_files.
        output_mount_point - where output files should be written in the Docker container. Files
            written anywhere else will not be transferred from the compute site to S3 / Minio.
            Must start from the container root and include at least one directory when resolved.
            The CTS default is /output_files.
        refdata_mount_point - where reference data should be mounted in the Docker container.
            Must start from the container root and include at least one directory when resolved.
        args - a list of arguments to provide to the container's entrypoint. See example below.
        num_containers - the number of containers to run for the job. Files are split evenly among
            containers when the InsertFiles directive is used in the argument list.
        cpus - the number of cpus to allocate per container for the job.
        memory - the amount of memory to allocate per container - either as the number of bytes
             or a specification string such as 100MB, 2GB, etc.
        runtime - the runtime required for each container as the number of seconds or
            an ISO8601 duration string.
        log_body - log the request body JSON before sending the request to the CTS.
        
        `args` example:
        
        The `args` argument is the list of arguments appended to the container's entrypoint.
        The contents of the list can either be literal strings or special classes that
        dynamically insert contents into the string. There are currently 2 special classes:
        
        InsertFiles - inserts the input files, or a subset of the input files if there is more
            than 1 container, into the command line.
        InsertContainerNumber - inserts the container number, with an optional prefix or suffix,
            into the command line.
            
        As an example:
        
        [
            "subcommand",
            "--flag", "flagvalue",
            "--output_dir", InsertContainerNumber(prefix="container_"),
            "--flag2, "flag2value",
            InsertFiles.spacesep(),
        ]
        
        See the documentation for the special classes for more information.
        """
        # TODO FUTURE add a method for listing images. Doesn't seem very useful
        # TODO FUTURE support environment when needed, seems like an unusual feature
        # TODO FUTURE support input_roots when needed, seems like an unusual feature
        # TODO FUTURE could do more input checking here, but the server already does it...
        #             could take trivial load of the server by doing a better job here
        if not input_files:
            raise ValueError("At least one input file is required")
        params = {}
        # started DRYing the below up but readability was worse
        if input_mount_point:  # could add this logic to require string. meh.
            params["input_mount_point"] = _require_string(
                input_mount_point, "input_mount_point", optional=True
            )
        if output_mount_point:
            params["output_mount_point"] = _require_string(
                output_mount_point, "output_mount_point", optional=True
            )
        if refdata_mount_point:
            params["refdata_mount_point"] = _require_string(
                refdata_mount_point, "refdata_mount_point", optional=True
            )
        if args:
            new_args = []
            for i, a in enumerate(args):
                if isinstance(a, str):
                    new_args.append(a)
                elif isinstance(a, (InsertContainerNumber, InsertFiles)):
                    new_args.append(a.render())
                else:
                    raise ValueError(f"Invalid type in args at position {i}: {type(a).__name__}")
            params["args"] = new_args
        job = {
            "cluster": _require_string(cluster, "cluster"),
            "image": _require_string(image, "image"),
            "params": params,
            "num_containers": _require_int(num_containers, 1, "num_containers"),
            "cpus": _require_int(cpus, 1, "cpus"),
            "memory": memory,  # checking this w/o pydantic would suuuck
            "runtime": runtime,   # checking this w/o pydantic would suuuck
            "output_dir": _require_string(output_dir, "output_dir"),
            "input_files": input_files,
        }
        if log_body:
            self._log.info(f"Job request to be sent to CTS:\n{json.dumps(job, indent=4)}")
        # Again for submission we just fail w/o retry. It's the polling where retries are important
        # Add later if needed
        job_id = self._cts_request("jobs", body=job)["job_id"]
        return Job(job_id, self)


class Job:
    """
    A class representing a CTS job.
    
    Instance variables:
    
    id - the ID of the job.
    """
    
    def __init__(self, job_id: str, client: CTSClient):
        """This class is not expected to be initialized manually."""
        self.id = job_id
        self._client = client

    def get_job_status(self) -> dict[str, Any]:
        """
        Get minimal details about the job. Returns the same data structure as the CTS job status
        endpoint.
        """
        return self._get_job(True)

    def get_job(self) -> dict[str, Any]:
        """
        Get details about the job. Returns the same data structure as the CTS jobs endpoint.
        """
        return self._get_job(False)
    
    def _get_job(self, status: bool):
        # Since it's expected that users will be manually calling these method, don't
        # worry about retries for now. Add if needed
        # SWitch to pydantic models if needed. Not sure it's actually helpful here
        stat = "/status" if status else ""
        return self._client._cts_request(f"jobs/{self.id}{stat}")
    
    _BACKOFF = [10, 30, 60, 120, 300, 600]
    
    def wait_for_completion(
            self,
            *,
            timeout_sec: int = 0,
            wait_for_event_importer: bool = False,
            log_state_changes: bool = True,
            log_polling: bool = False,
    ) -> dict[str, Any]:
        """
        Wait for the job to complete or error out.
        
        Note that depending on queue times at the remote compute site, this operation may take
        hours or days.
        
        timeout_sec - throw a TimeoutError if the job has not completed by this number of seconds.
            If < 1, a timeout will never occur.
        wait_for_event_importer - WARNING: if the CTS job image does not have an event importer
            configured, this method will either block forever or timeout if a timeout is set.
            If True, wait for the CDM Spark Events Processor to process the job's data after
            the job is complete. Will wait until the importer has reported either completion
            or an error to the CTS, or the job is in the error state.
        log_state_changes - emit a log when the job state changes.
        log_polling - emit a log when polling the job state.
        
        Returns minimal details about the job. Returns the same data structure as the CTS job
        status endpoint.
        """
        # TODO TEST logging throughout this method.
        #           pytest w/ --log-cli-level=INFO works for manual checks
        # TODO TEST backoffs, will need mocks most likely
        # TODO NEXT wait for event processor import to be done
        job_state = None
        backoff_index = -1
        start_time = time.monotonic()
        while True:
            backoff_index, backoff = self._get_next_backoff(backoff_index)
            try:
                res = self._client._cts_request(f"jobs/{self.id}/status", fail_on_500=False)
                state = res["state"]
                if log_state_changes and job_state and job_state != state:
                    self._client._log.info(
                        f"Job {self.id} transitioned from {job_state} to {state}"
                    )
                if state == "error":
                    return res
                elif wait_for_event_importer:
                    # if either of the event completion keys are in the admin_meta dict
                    if _EVENT_COMPLETION_KEYS & res["admin_meta"].keys():
                        return res
                    # otherwise keep polling even if job is complete
                elif state == "complete":
                    return res
                if log_polling:
                    self._client._log.info(f"Polled job {self.id}, polling again in {backoff}s")
                job_state = state
            except CTSClientError:
                raise  # unrecoverable
            except Exception:
                # Primarily expecting connection errors and _PotentiallyRecoverableError here.
                # This may catch some unexpected errors that should be fatal.
                # If such cases arise, this block should be made more specific.
                self._client._log.exception(
                    f"Fetching job {self.id} failed - retrying in {backoff}s"
                )
            if timeout_sec > 0:
                elapsed = time.monotonic() - start_time
                if elapsed + backoff > timeout_sec:
                    raise TimeoutError(
                        f"Timed out waiting for job {self.id} after {timeout_sec} seconds."
                    )
            time.sleep(backoff)
        
    def _get_next_backoff(self, backoff_index: int):
        bi = min(backoff_index + 1, len(self._BACKOFF) -1)
        return bi, self._BACKOFF[bi]


def _require_string(putative: Any, name: str, optional: bool = False) -> str:
    if not isinstance(putative, str) or not putative.strip():
        raise ValueError(
            f"The '{name}' string argument{', if provided,' if optional else ' is required and'} "
            + "cannot be a whitespace only string"
        )
    return putative.strip()


def _require_int(putative: Any, minimum: int, name: str):
    if not isinstance(putative, int) or putative < minimum:
        raise ValueError(f"The '{name}' argument is required and must be an integer >= {minimum}")
    return putative


class CTSClientError(Exception):
    """ The root error class for the CTS Client. """


class InvalidTokenError(CTSClientError):
    """ Thrown when the provided token is invalid. """


class NoSuchJobError(CTSClientError):
    """ Thrown when the requested job does not exist. """


class UnauthorizedError(CTSClientError):
    """ Thrown when the user is not allowed to perform the requetsed action. """


class SubmissionError(CTSClientError):
    """ Thrown when a job submission fails. """


class SubmissionStructureError(SubmissionError):
    """
    Thrown when the data structure submitted to the server for a job submission is incorrect.
    
    Instance variables:
    validation_errors - the error detail returned by the server that was generated by pydantic.
    """
    def __init__(self, message: str, validation_errors: list[dict[str, Any]]):
        super().__init__(message)
        self._message = message
        self.validation_errors = validation_errors

    def __str__(self):
        return f"{self._message}:\n{json.dumps(self.validation_errors, indent=4)}"


class UnexpectedServerResponseError(CTSClientError):
    """ Thrown when the server returns an unexpected response. """


class _PotentiallyRecoverableError(Exception):  # used for retries
    pass
