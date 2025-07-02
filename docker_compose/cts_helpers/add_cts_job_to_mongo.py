"""
*Very* quick n dirty method to put something resembling a CTS job in mongodb such that it'll
pass Pydantic validation on its way out of the CTS.

Assuming a docker compose context, invoke like:

docker compose exec cdm-task-service python /cts_helpers/add_cts_job_to_mongo.py \
    my_job_id complete ubuntu sha256:digest_here \
    mybucket/file1 crc1aaaaaaaa \
    mybucket/file2 crc2aaaaaaaa
"""

# copied from https://github.com/kbase/cdm-spark-events/blob/main/docker_compose/cts_helpers/add_cts_job_to_mongo.py

# Lots of potential improvements here, handle them as needed

import asyncio
from datetime import datetime, timezone
import json
from motor.motor_asyncio import AsyncIOMotorClient
import os
import sys
from typing import Any


def encode_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def main():
    # Maybe should think about mounting a local dir into the container so this stuff can be
    # specified in a yaml file or something
    if len(sys.argv) < 7:
        raise ValueError("Needs to have at least 6 args")
    if (len(sys.argv) - 1) % 2 != 0:
        raise ValueError("Each file must be followed by a crc64nvme base54 encoded checksum")
    # TODO TEST_UX use argparse to make this less gross
    # TODO TEST_MONGO support creds
    mongo_host = os.environ["KBCTS_MONGO_HOST"]
    mongo_db = os.environ["KBCTS_MONGO_DB"]
    job_id = sys.argv[1]
    job_state = sys.argv[2]
    image = sys.argv[3]
    image_digest = sys.argv[4]
    files = []
    for i in range(5, len(sys.argv), 2):
        if len(sys.argv[i + 1]) != 12:
            raise ValueError(f"crc64nvme must be 12 chars exactly: {sys.argv[i + 1]}")
        files.append({
            "file": sys.argv[i],
            "crc64nvme": sys.argv[i + 1]
        })
    now = datetime.now(timezone.utc)
    job = {
        "id": job_id,
        "job_input": {
            "cluster": "perlmutter-jaws",
            "image": f"{image}@{image_digest}",
            "params": {
                "args": ["foo", "bar"],
            },
            "output_dir": "mybucket/out",
            "input_files": [{
                "file": "mybucket/input_file",
                "crc64nvme": "aaaaaaaaaaaa",
            }],
        },
        "user": "some_user",
        "image": {
            "registered_by": "some_user",
            "registered_on": now,
            "name": image,
            "digest": image_digest,
            "entrypoint": ["command"],
            "tag": "fake_tag",
        },
        "input_file_count": 1,
        "state": job_state,
        # add some fake TTs for fun
        "transition_times": [
            {
                "state": "created",
                "time": now,
                "trans_id": "tid",
                "notif_sent": True,
            },
            {
                "state": "complete",
                "time": now,
                "trans_id": "tid2",
                "notif_sent": True,
            },
        ],
        "output_file_count": len(files),
        "outputs": files,
    }
    asyncio.run(save_job(mongo_host, mongo_db, job))


async def save_job(mongo_host: str, mongo_db: str, job: dict[str, Any]):
    print("Saving job to mongo:\n")
    print(json.dumps(job, indent=4, default=encode_datetime))
    
    client = AsyncIOMotorClient(mongo_host, tz_aware=True)
    try:
        await client[mongo_db].jobs.insert_one(job)
    finally:
        client.close()
    print("Done, kthxbye")


if __name__ == "__main__":
    main()
