from awscrt import checksums as awschecksums
import base64
import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError
import io
import json
import os
from pymongo import MongoClient
from pymongo.database import Database
import pytest
import requests
import subprocess
import time


# settings are coming from the docker-compose file

_COMPOSE_FILE = "docker-compose.yaml"
_COMPOSE_PROJECT_NAME = "cts_client_tests"
_CTS_SERVICE_NAME = "cdm-task-service"

CTS_URL = "http://localhost:5000/"
AUTH_URL = "http://localhost:50001/testmode/api/V2/"

HTTPSTAT_US_URL = "http://localhost:5001/"
CTS_FAIL_URL = auth_url = "http://localhost:5010/"

MONGO_HOST = "localhost:27017"
MONGO_DB = "cdmtaskservice"
MONGO_DB_AUTH = "cdm_client_test"

S3_HOST = "http://localhost:9000"
S3_KEY = "cts"
S3_SECRET = "ctspassword"
CTS_BUCKET = "cts-data"
CTS_BUCKET_LOGS = "cts-logs"

HAS_NERSC_ACCOUNT_ROLE = "HAS_NERSC_ACCOUNT"
KBASE_STAFF_ROLE = "KBASE_STAFF"


def _wait_for_services(timeout: int = 30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            res = subprocess.run(
                [
                    "docker", "compose",
                    "-f", _COMPOSE_FILE, "-p", _COMPOSE_PROJECT_NAME,
                    "ps", "--format", "json"],
                capture_output=True,
                check=True,
            )
            status = None
            for jsonline in res.stdout.decode("utf-8").split("\n"):
                if jsonline.strip():
                    j = json.loads(jsonline.strip())
                    if j["Service"] == _CTS_SERVICE_NAME:
                        status = j["Health"]
                        if status == "healthy":
                            print(f"Service {_CTS_SERVICE_NAME} is healthy.")
                            return
            status = status if status else "Container not yet started"
            print(f"Waiting for {_CTS_SERVICE_NAME} to become healthy... (current: {status})")
        except subprocess.CalledProcessError as e:
            print(f"Error waiting for {_CTS_SERVICE_NAME}: {e.stderr.strip()}")
        time.sleep(2)

    raise TimeoutError(f"{_CTS_SERVICE_NAME} did not become healthy within {timeout} seconds.")


def _run_dc(env, *args):
    subprocess.run(
        [
            "docker", "compose",
            "-f", _COMPOSE_FILE, "-p", _COMPOSE_PROJECT_NAME,
        ] + list(args),
        check=True,
        env=env
    )


def _clear_auth_db():
    mc =  MongoClient(MONGO_HOST)
    db = mc[MONGO_DB_AUTH]
    # don't drop db since that drops indexes
    for name in db.list_collection_names():
        if not name.startswith("system."):
            # don't drop collection since that drops indexes
            db.get_collection(name).delete_many({})


@pytest.fixture(scope="session", autouse=True)
def docker_compose():
    env = os.environ.copy()
    print("Starting docker-compose...")
    try:
        _run_dc(env, "up", "-d", "--build")
        _wait_for_services()
        _clear_auth_db()  # in case the compose was left up
        yield  # run the tests
        logarg = os.environ.get("CTS_TEST_DUMP_LOGS")
        if logarg:
            if logarg.strip():
                _run_dc(env, "logs", logarg)
            else:
                _run_dc(env, "logs")
    finally:
        if not os.environ.get("CTS_CLIENT_TEST_LEAVE_COMPOSE_UP"):
            print("Stopping docker-compose...")
            _run_dc(env, "down")


@pytest.fixture(scope="session", autouse=True)
def auth_user(docker_compose) -> tuple[str, str]:  # username, token
    res = requests.post(f"{AUTH_URL}testmodeonly/user", json={"user": "myuser", "display": "foo"})
    res.raise_for_status()
    
    for r in [KBASE_STAFF_ROLE, HAS_NERSC_ACCOUNT_ROLE]:
        res = requests.post(f"{AUTH_URL}testmodeonly/customroles", json={"id": r, "desc": "foo"})
        res.raise_for_status()

    res = requests.put(
        f"{AUTH_URL}testmodeonly/userroles",
        json={"user": "myuser", "customroles": [KBASE_STAFF_ROLE, HAS_NERSC_ACCOUNT_ROLE]},
    )
    res.raise_for_status()

    res = requests.post(f"{AUTH_URL}testmodeonly/token", json={"user": "myuser", "type": "Dev"})
    res.raise_for_status()

    return "myuser", res.json()["token"]


@pytest.fixture(scope="session", autouse=True)
def disallowed_auth_user(docker_compose) -> tuple[str, str]:  # username, token
    # creates a user that isn't allowed to use the CTS because it doesn't have the right roles
    res = requests.post(f"{AUTH_URL}testmodeonly/user", json={"user": "baduser", "display": "foo"})
    res.raise_for_status()
    
    res = requests.post(f"{AUTH_URL}testmodeonly/token", json={"user": "baduser", "type": "Dev"})
    res.raise_for_status()
    return "myuser", res.json()["token"]


@pytest.fixture(scope="session")
def mongo_setup(docker_compose) -> Database:
    client = MongoClient(MONGO_HOST)
    yield client [MONGO_DB]
    client.close()


@pytest.fixture()
def mongo_db(mongo_setup):
    # don't drop indexes, only created on server startup
    for coll_name in mongo_setup.list_collection_names():
        if not coll_name.startswith("system."):
            mongo_setup[coll_name].delete_many({})
    return mongo_setup


def crc64nvme(data: str) -> bytes:  # helper to calc checksum for hardcoding
    return base64.b64encode(awschecksums.crc64nvme(data.encode("utf-8"), 0).to_bytes(8)).decode()


def put_object(client: BaseClient, key: str, data: str, crc64nvme: str):
    kwargs = {
        "Bucket": CTS_BUCKET,
        "Key": key,
        "Body": io.BytesIO(data.encode("utf-8")),
    }
    if crc64nvme:
        kwargs["ChecksumCRC64NVME"] = crc64nvme
    client.put_object(**kwargs)


@pytest.fixture(scope="session", autouse=True)
def s3_client() -> BaseClient:
    client: BaseClient = boto3.client(
        "s3",
        endpoint_url=S3_HOST,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
    )
    try:
        client.head_bucket(Bucket=CTS_BUCKET)
    except ClientError:
        client.create_bucket(Bucket=CTS_BUCKET)
    put_object(client, "infile", "foo", "5O33DmauDQI=")
    put_object(client, "infile2", "bar", "ikLs7sFMNGo=")
    put_object(client, "no_checksum", "baz", None)
    yield client
