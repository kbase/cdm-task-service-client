from conftest import CTS_URL, auth_user
import requests


def test_whoami(auth_user):
    # TODO test make the client do this
    res = requests.get(f"{CTS_URL}/whoami", headers={"Authorization": f"Bearer {auth_user[1]}"})
    assert res.json() == {"user": auth_user[0], "is_service_admin": False}

