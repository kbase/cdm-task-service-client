import copy
from datetime import datetime, timezone
from pymongo.database import Database
import pytest
import threading
import time
from typing import Any

from conftest import CTS_URL, CTS_FAIL_URL, HTTPSTAT_US_URL

from cdmtaskserviceclient import client
from cdmtaskserviceclient.client import (
    CTSClient,
    InvalidTokenError,
    NoSuchJobError,
    SubmissionError,
    SubmissionStructureError,
    UnauthorizedError,
    UnexpectedServerResponseError,
)


# TODO TEST add a test for the case where an image requires refdata but staging isn't complete.
#           Pretty edgy case and needs a bit of setup

VERSION = "0.2.1"


def test_version():
    assert client.__version__ == VERSION
    assert CTSClient.__version__ == VERSION


def test_insert_files():
    expected_space = {"type":"input_files", 'input_files_format':'space_separated_list'}
    im = CTSClient.insert_files()
    assert im == expected_space
    
    im = CTSClient.insert_files(mode="space")
    assert im == expected_space
    
    im = CTSClient.insert_files(mode="comma")
    assert im == {"type": "input_files", 'input_files_format': 'comma_separated_list'}


def test_insert_files_fail():
    with pytest.raises(ValueError) as e:
        CTSClient.insert_files(mode="tab")
    assert str(e.value) == "mode must be 'space' or 'comma'"


def test_insert_container_number():
    icn = CTSClient.insert_container_number()
    assert icn == {
        "type": "container_number", "container_num_prefix": None, "container_num_suffix": None
    }
    
    icn = CTSClient.insert_container_number(prefix="   \t  ", suffix=" \n  ")
    assert icn == {
        "type": "container_number", "container_num_prefix": None, "container_num_suffix": None
    }
    
    icn = CTSClient.insert_container_number(prefix="\tfoo   ", suffix="   bar  \t     ")
    assert icn == {
        "type": "container_number", "container_num_prefix": "foo", "container_num_suffix": "bar"
    }


def test_constructor(auth_user):
    cli = CTSClient(f"   \t{auth_user[1]}   ", url=f"   {CTS_URL}\t   ")
    assert cli.user == auth_user[0]


def test_constructor_fail_no_token():
    for t in [None, "  \t  ", 3]:
        with pytest.raises(ValueError) as e:
            CTSClient(t)
        assert str(e.value) == (
            "The 'token' string argument is required and cannot be a whitespace only string"
        )


def test_constructor_fail_no_url():
    for u in [None, "  \t  ", 3]:
        with pytest.raises(ValueError) as e:
            CTSClient("token", url=u)
        assert str(e.value) == (
            "The 'url' string argument is required and cannot be a whitespace only string"
        )


def test_constructor_fail_error_5XX_response():
    with pytest.raises(UnexpectedServerResponseError) as e:
        CTSClient("token", url="https://ci.kbase.us/services/ws")
    assert str(e.value) == "Error response (500) from the CTS"


def test_constructor_fail_error_3XX_response():
    with pytest.raises(UnexpectedServerResponseError) as e:
        CTSClient("token", url=f"{HTTPSTAT_US_URL}/309")
    assert str(e.value) == ("Unexpected response (309) from the CTS")


def test_constructor_fail_error_4XX_response_not_json():
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


# TODO TEST role checks have been moved to the flow providers, which are not available when
#           NERSC and KBase are skipped. Not sure how to test this.


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


def _setup_image(mongo_db: Database):
    mongo_db.images.insert_one({
        "registered_by": "dude",
        "registered_on": datetime(2024, 10, 24, 22, 35, 40, tzinfo=timezone.utc),
        "name": "ghcr.io/kbasetest/cdm_checkm2",
        "digest": "sha256:c9291c94c382b88975184203100d119cba865c1be91b1c5891749ee02193d380",
        "entrypoint": [
          "checkm2",
          "predict"
        ],
        "tag": "0.3.0",
    })


def _clean_job(job: dict[str, Any]) -> dict[str, Any]:
    """ Remove timestamps and IDs from the job data as they change from test run to test run. """
    newjob = copy.deepcopy(job)
    del newjob["id"]
    for tt in newjob["transition_times"]:
        del tt["time"]
    return newjob


def test_submit_minimal(auth_user, mongo_db):
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    job1 = cli.submit_job(
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-data/infile"],
        "cts-data/out"
    )
    job2 = cli.get_job_by_id(job1.id)
    
    for job in (job1, job2):
        stat = job.get_job_status()
        j = job.get_job()
        assert _clean_job(stat) == {
            "user": "myuser",
            "admin_meta": {},
            "state": "created",
            "transition_times": [{"state": "created"}]
        }
        assert _clean_job(j) == {
            "user": "myuser",
            "admin_meta": {},
            "state": "created",
            "transition_times": [{"state": "created"}],
            "job_input": {
                "cluster": "perlmutter-jaws",
                "image": "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
                "params": {
                    "input_mount_point": "/input_files",
                    "output_mount_point": "/output_files",
                    "declobber": False,
                },
                "num_containers": 1,
                "cpus": 1,
                "memory": 10000000,
                "runtime": "PT5M",
                "output_dir": "cts-data/out/",
                "input_files": [
                    {
                        "file": "cts-data/infile",
                        "crc64nvme": "5O33DmauDQI="
                    }
                ]
            },
            "image": {
                "registered_by": "dude",
                "registered_on": "2024-10-24T22:35:40Z",
                "name": "ghcr.io/kbasetest/cdm_checkm2",
                "digest":
                    "sha256:c9291c94c382b88975184203100d119cba865c1be91b1c5891749ee02193d380",
                "entrypoint": ["checkm2", "predict"],
                "tag": "0.3.0"
            },
            "input_file_count": 1
        }


def test_submit_maximal(auth_user, mongo_db):
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    job1 = cli.submit_job(
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-data/infile", "cts-data/infile2"],
        "cts-data/out2",
        cluster="perlmutter-jaws",  # only value that won't cause an error currently
        input_mount_point="/inmount",
        output_mount_point="/outmount",
        refdata_mount_point="/refmount",
        args=[
            "foo",
            cli.insert_files(),
            "--bar",
            cli.insert_container_number(suffix="_containers_ah_ha_ha")
        ],
        num_containers=2,
        cpus=24,
        memory="100GB",
        runtime="PT3H",
        log_body=True,  # TODO TEST this actually works
    )
    job2 = cli.get_job_by_id(job1.id)
    
    for job in (job1, job2):
        stat = job.get_job_status()
        j = job.get_job()
        assert _clean_job(stat) == {
            "user": "myuser",
            "admin_meta": {},
            "state": "created",
            "transition_times": [{"state": "created"}]
        }
        assert _clean_job(j) == {
            "user": "myuser",
            "admin_meta": {},
            "state": "created",
            "transition_times": [{"state": "created"}],
            "job_input": {
                "cluster": "perlmutter-jaws",
                "image": "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
                "params": {
                    "input_mount_point": "/inmount",
                    "output_mount_point": "/outmount",
                    "refdata_mount_point": "/refmount",
                    "declobber": False,
                    "args": [
                        "foo",
                        {
                            "type": "input_files",
                            "input_files_format": "space_separated_list"
                        },
                        "--bar",
                        {
                            "type": "container_number",
                            "container_num_suffix": "_containers_ah_ha_ha"
                        }
                    ]
                },
                "num_containers": 2,
                "cpus": 24,
                "memory": 100000000000,
                "runtime": "PT3H",
                "output_dir": "cts-data/out2/",
                "input_files": [
                    {
                        "file": "cts-data/infile",
                        "crc64nvme": "5O33DmauDQI="
                    },
                    {
                        "file": "cts-data/infile2",
                        "crc64nvme": "ikLs7sFMNGo="
                    }
                ]
            },
            "image": {
                "registered_by": "dude",
                "registered_on": "2024-10-24T22:35:40Z",
                "name": "ghcr.io/kbasetest/cdm_checkm2",
                "digest":
                    "sha256:c9291c94c382b88975184203100d119cba865c1be91b1c5891749ee02193d380",
                "entrypoint": ["checkm2", "predict"],
                "tag": "0.3.0"
            },
            "input_file_count": 2
        }


def _submit_fail(cli: CTSClient, msg: str, errclass: Exception, *args, **kwargs):
    with pytest.raises(errclass) as got:
        cli.submit_job(*args, **kwargs)
    assert str(got.value) == msg


def test_submit_fail_bad_args(auth_user):
    cli = CTSClient(auth_user[1], url=CTS_URL)
    imgerr = "The 'image' string argument is required and cannot be a whitespace only string"
    _submit_fail(cli, imgerr, ValueError, None, ["foo"], "bar")
    _submit_fail(cli, imgerr, ValueError, "  \t   ", ["foo"], "bar")
    
    fileserr = "At least one input file is required"
    _submit_fail(cli, fileserr, ValueError, "image", None, "bar")
    _submit_fail(cli, fileserr, ValueError, "image", [], "bar")

    outerr = "The 'output_dir' string argument is required and cannot be a whitespace only string"
    _submit_fail(cli, outerr, ValueError, "image", ["foo"], None)
    _submit_fail(cli, outerr, ValueError, "image", ["foo"], "  \t   ")

    clerr = "The 'cluster' string argument is required and cannot be a whitespace only string"
    _submit_fail(cli, clerr, ValueError, "image", ["foo"], "bar", cluster=None)
    _submit_fail(cli, clerr, ValueError, "image", ["foo"], "bar", cluster="   \t   ")
    
    mnterr = "The '{}' string argument, if provided, cannot be a whitespace only string"
    inmnterr = mnterr.format("input_mount_point")
    _submit_fail(cli, inmnterr, ValueError, "image", ["foo"], "bar", input_mount_point="   \t ")
    outmnterr = mnterr.format("output_mount_point")
    _submit_fail(cli, outmnterr, ValueError, "image", ["foo"], "bar", output_mount_point="   \t ")
    refmnterr = mnterr.format("refdata_mount_point")
    _submit_fail(cli, refmnterr, ValueError, "image", ["foo"], "bar", refdata_mount_point="   \t ")

    argerr = "Invalid type in args at position 1: int"
    _submit_fail(cli, argerr, ValueError, "image", ["foo"], "bar", args=["foo", 1])

    numerr = "The 'num_containers' argument is required and must be an integer >= 1"
    _submit_fail(cli, numerr, ValueError, "image", ["foo"], "bar", num_containers=None)
    _submit_fail(cli, numerr, ValueError, "image", ["foo"], "bar", num_containers="one")
    _submit_fail(cli, numerr, ValueError, "image", ["foo"], "bar", num_containers=0.1)
    
    cpuerr = "The 'cpus' argument is required and must be an integer >= 1"
    _submit_fail(cli, cpuerr, ValueError, "image", ["foo"], "bar", cpus=None)
    _submit_fail(cli, cpuerr, ValueError, "image", ["foo"], "bar", cpus="two")
    _submit_fail(cli, cpuerr, ValueError, "image", ["foo"], "bar", cpus=-1)


def test_submit_fail_serverside_structure_validation(auth_user):
    cli = CTSClient(auth_user[1], url=CTS_URL)
    # not super happy about this but doing better would probably not have a positive ROI
    err = """
The CDM Task Service rejected the job submission request:
[
    {
        "type": "value_error",
        "loc": [
            "body",
            "output_dir"
        ],
        "msg": "Value error, Path 'baz' must start with the s3 bucket and include a key",
        "input": "baz",
        "ctx": {
            "error": {}
        }
    }
]
""".strip()
    _submit_fail(cli, err, SubmissionStructureError, "image", ["foo/bar"], "baz")


def test_submit_fail_serverside_illegal_parameter_no_checksum(auth_user, mongo_db):
    # Tests the Illegal Parameter error code.
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    _submit_fail(
        cli,
        "The S3 path 'cts-data/no_checksum' does not have a CRC64/NVME checksum",
        SubmissionError,
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-data/no_checksum"],
        "cts-data/out"
    )


def test_submit_fail_serverside_no_such_image(auth_user):
    # Tests the no such image error code.
    cli = CTSClient(auth_user[1], url=CTS_URL)
    err = "No image docker.io/fake/image with tag 'latest' exists in the system."
    _submit_fail(cli, err, SubmissionError, "fake/image", ["cts-data/infile"], "cts-data/out")


def test_submit_fail_serverside_bad_image_name(auth_user):
    # Tests the no such image error code.
    cli = CTSClient(auth_user[1], url=CTS_URL)
    err = "Illegal character in image name 'fake/ima&ge': '&'"
    _submit_fail(cli, err, SubmissionError, "fake/ima&ge", ["cts-data/infile"], "cts-data/out")


def test_submit_fail_serverside_write_to_log_path(auth_user, mongo_db):
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    _submit_fail(
        cli,
        "Jobs may not write to the log path cts-logs/container_logs/",
        SubmissionError,
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-data/infile"],
        "cts-logs/container_logs/out"
    )


def test_submit_fail_serverside_write_to_missing_bucket(auth_user, mongo_db):
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    _submit_fail(
        cli,
        "Write access denied to path 'cts-fake/out/' on the s3 system",
        SubmissionError,
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-data/infile"],
        "cts-fake/out"
    )


def test_submit_fail_serverside_missing_input_bucket(auth_user, mongo_db):
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    _submit_fail(
        cli,
        "Read access denied to path 'cts-fake/infile' on the s3 system",
        SubmissionError,
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-fake/infile"],
        "cts-data/out"
    )


def test_submit_fail_serverside_missing_input_file(auth_user, mongo_db):
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    _submit_fail(
        cli,
        "The path 'cts-data/nofile' was not found on the s3 system",
        SubmissionError,
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-data/nofile"],
        "cts-data/out"
    )


def test_submit_fail_serverside_job_flow_not_available(auth_user, mongo_db):
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_FAIL_URL)
    _submit_fail(
        cli,
        "Job flow for cluster perlmutter-jaws is not registered",
        SubmissionError,
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-data/infile"],
        "cts-data/out"
    )


def _get_job_by_id_fail(cli, msg, job_id):
    with pytest.raises(ValueError) as got:
        cli.get_job_by_id(job_id)
    assert str(got.value) == msg


def test_get_job_by_id_fail_bad_args(auth_user):
    cli = CTSClient(auth_user[1], url=CTS_URL)
    err = "The 'job_id' string argument is required and cannot be a whitespace only string"
    _get_job_by_id_fail(cli, err, None)
    _get_job_by_id_fail(cli, err, "    \t    ")


def test_get_job_and_get_job_status_fail_bad_job_id(auth_user):
    cli = CTSClient(auth_user[1], url=CTS_URL)
    job = cli.get_job_by_id("fake")
    msg = "No job with ID 'fake' exists"
    with pytest.raises(NoSuchJobError) as got:
        job.get_job()
    assert str(got.value) == msg
    with pytest.raises(NoSuchJobError) as got:
        job.get_job_status()
    assert str(got.value) == msg


def test_get_job_and_get_job_status_fail_unauthed(auth_user, disallowed_auth_user, mongo_db):
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    job = cli.submit_job(
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-data/infile"],
        "cts-data/out"
    )
    cli = CTSClient(disallowed_auth_user[1], url=CTS_URL)
    job = cli.get_job_by_id(job.id)
    err = f"User baduser may not access job {job.id}"
    with pytest.raises(UnauthorizedError, match=err) as got:
        job.get_job()
    with pytest.raises(UnauthorizedError, match=err) as got:
        job.get_job_status()


def test_wait_for_completion_success(auth_user, mongo_db):
    _wait_for_completion("complete", auth_user, mongo_db)


def test_wait_for_completion_error(auth_user, mongo_db):
    _wait_for_completion("error", auth_user, mongo_db)


def _wait_for_completion(final_state, auth_user, mongo_db):
    # might want to patch the backoff times to speed these tests up.
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    # easier to just create a job than use mongo directly
    job = cli.submit_job(
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-data/infile"],
        "cts-data/out"
    )
    res = {"ret": {"id": None, "transition_times": []}}  # make the _clean_job method not fail
    def poller():
        try:
            res["ret"] = job.wait_for_completion(log_polling=True)
        except Exception as e:
            res["err"] = e
    poll_thread = threading.Thread(target=poller)
    poll_thread.start()
    
    time.sleep(5)
    # TODO TEST that the logs for this change shows up. manually checked they work
    # currently can use --log-cli-level=INFO w/ pytest to see logs
    mongo_db.jobs.update_one({"id": job.id}, {"$set": {"state": "job_submitted"}})
    time.sleep(10)
    mongo_db.jobs.update_one({"id": job.id}, {"$set": {"state": final_state}})
    
    poll_thread.join()
    res["ret"] = _clean_job(res["ret"])
    assert res == {
        "ret":
            {
                "user": "myuser",
                "admin_meta": {},
                "state": final_state,
                "transition_times": [{"state": "created"}]
            }
    }


def test_wait_for_completion_fail_bad_job_id(auth_user, mongo_db):
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    job = cli.get_job_by_id("fake")
    with pytest.raises(NoSuchJobError) as got:
        job.wait_for_completion()
    assert str(got.value) == "No job with ID 'fake' exists"


def test_wait_for_completion_fail_timeout(auth_user, mongo_db):
    # might want to patch the backoff times to speed these tests up.
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    # easier to just create a job than use mongo directly
    job = cli.submit_job(
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-data/infile"],
        "cts-data/out"
    )
    res = {}
    def poller():
        try:
            res["ret"] = job.wait_for_completion(log_polling=True, timeout_sec=12)
        except Exception as e:
            res["err"] = e
    poll_thread = threading.Thread(target=poller)
    poll_thread.start()
    
    time.sleep(13)
    poll_thread.join()
    e = res["err"]
    assert type(e) == TimeoutError
    assert str(e) == f'Timed out waiting for job {job.id} after 12 seconds.'


def test_wait_for_completion_fail_timeout_with_bad_url(auth_user, mongo_db):
    # tests a timeout and also tests retries after a 500, but you have to check the logs
    # manually to see that happening
    # might want to patch the backoff times to speed these tests up.
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    # easier to just create a job than use mongo directly
    job = cli.submit_job(
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-data/infile"],
        "cts-data/out"
    )
    cli._url = f"{HTTPSTAT_US_URL}/500"  # naughty
    res = {}
    def poller():
        try:
            res["ret"] = job.wait_for_completion(log_polling=True, timeout_sec=17)
        except Exception as e:
            res["err"] = e
    poll_thread = threading.Thread(target=poller)
    poll_thread.start()
    
    # TODO TEST check exceptions are being logged
    time.sleep(13)
    poll_thread.join()
    e = res["err"]
    assert type(e) == TimeoutError
    assert str(e) == f'Timed out waiting for job {job.id} after 17 seconds.'


def test_wait_for_completion_importer_success(auth_user, mongo_db):
    _wait_for_completion_importer("cse_event_processing_complete", auth_user, mongo_db)


def test_wait_for_completion_importer_error(auth_user, mongo_db):
    _wait_for_completion_importer("cse_event_processing_error", auth_user, mongo_db)


def test_wait_for_completion_importer_no_operation(auth_user, mongo_db):
    _wait_for_completion_importer("cse_event_processing_no_operation", auth_user, mongo_db)


def _wait_for_completion_importer(importer_key, auth_user, mongo_db):
    # might want to patch the backoff times to speed these tests up.
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    # easier to just create a job than use mongo directly
    job = cli.submit_job(
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-data/infile"],
        "cts-data/out"
    )
    res = {"ret": {"id": None, "transition_times": []}}  # make the _clean_job method not fail
    def poller():
        try:
            res["ret"] = job.wait_for_completion(log_polling=True, wait_for_event_importer=True)
        except Exception as e:
            res["err"] = e
    poll_thread = threading.Thread(target=poller)
    poll_thread.start()
    
    time.sleep(5)
    # TODO TEST that the logs for this change shows up. manually checked they work
    # currently can use --log-cli-level=INFO w/ pytest to see logs
    # should keep polling after job marked complete
    mongo_db.jobs.update_one({"id": job.id}, {"$set": {"state": "complete"}})
    time.sleep(10)
    mongo_db.jobs.update_one({"id": job.id}, {"$set": {
        f"admin_meta.{importer_key}": "date_would_go_here"}
    })
    
    poll_thread.join()
    res["ret"] = _clean_job(res["ret"])
    assert res == {
        "ret":
            {
                "user": "myuser",
                "admin_meta": {importer_key: "date_would_go_here"},
                "state": "complete",
                "transition_times": [{"state": "created"}]
            }
    }


def test_wait_for_completion_importer_job_error(auth_user, mongo_db):
    # might want to patch the backoff times to speed these tests up.
    _setup_image(mongo_db)
    cli = CTSClient(auth_user[1], url=CTS_URL)
    # easier to just create a job than use mongo directly
    job = cli.submit_job(
        "ghcr.io/kbasetest/cdm_checkm2:0.3.0",
        ["cts-data/infile"],
        "cts-data/out"
    )
    res = {"ret": {"id": None, "transition_times": []}}  # make the _clean_job method not fail
    def poller():
        try:
            res["ret"] = job.wait_for_completion(log_polling=True, wait_for_event_importer=True)
        except Exception as e:
            res["err"] = e
    poll_thread = threading.Thread(target=poller)
    poll_thread.start()
    
    time.sleep(5)
    # TODO TEST that the logs for this change shows up. manually checked they work
    # currently can use --log-cli-level=INFO w/ pytest to see logs
    # should stop polling after job marked error
    mongo_db.jobs.update_one({"id": job.id}, {"$set": {"state": "error"}})
    
    poll_thread.join()
    res["ret"] = _clean_job(res["ret"])
    assert res == {
        "ret":
            {
                "user": "myuser",
                "admin_meta": {},
                "state": "error",
                "transition_times": [{"state": "created"}]
            }
    }
