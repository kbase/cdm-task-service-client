import pytest
import os
import subprocess
import time
import json
import requests


_COMPOSE_FILE = "docker-compose.yaml"
_COMPOSE_PROJECT_NAME = "cts_client_tests"
_CTS_SERVICE_NAME = "cdm-task-service"
CTS_URL = auth_url = "http://localhost:5000/"
AUTH_URL = auth_url = "http://localhost:50001/testmode/api/V2/"
HTTPSTAT_US_URL = "http://localhost:5001/"
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
            if not status:
                raise ValueError(
                    f"Couldn't find the {_CTS_SERVICE_NAME} service in docker compose ps output"
                
            )
            print(f"Waiting for {_CTS_SERVICE_NAME} to become healthy... (current: {status})")
        except subprocess.CalledProcessError as e:
            print(f"Error waiting for {_CTS_SERVICE_NAME}: {e.stderr.strip()}")
        time.sleep(2)

    raise TimeoutError(f"{_CTS_SERVICE_NAME} did not become healthy within {timeout} seconds.")


@pytest.fixture(scope="session", autouse=True)
def docker_compose():
    env = os.environ.copy()
    print("Starting docker-compose...")
    try:
        subprocess.run(
            [
                "docker", "compose",
                "-f", _COMPOSE_FILE, "-p", _COMPOSE_PROJECT_NAME,
                "up", "-d", "--build"
            ],
            check=True,
            env=env
        )
        _wait_for_services()
        yield  # run the tests
    finally:
        print("Stopping docker-compose...")
        subprocess.run(
            [
                "docker", "compose",
                "-f", _COMPOSE_FILE, "-p", _COMPOSE_PROJECT_NAME,
                "down"],
            check=True,
            env=env
        )


@pytest.fixture(scope = "session", autouse=True)
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
