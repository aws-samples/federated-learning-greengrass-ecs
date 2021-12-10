# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import flwr as fl
import boto3
import os
import requests
import json
import logging
import sys

# Setup logging to stdout
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

token = os.getenv('TASK_TOKEN')
metadata_uri = os.getenv('ECS_CONTAINER_METADATA_URI_V4')
token_response = ""
if metadata_uri is not None:
    r = requests.get(f"{metadata_uri}/task")
    task_meta = r.json()
    logger.info(f"found task metadata: {task_meta}")
    token_response = json.dumps({
        "ip": task_meta['Containers'][0]['Networks'][0]['IPv4Addresses'][0],
        "taskArn": task_meta['TaskARN']
    })

if token is not None:
    stfn = boto3.client('stepfunctions')
    logger.info(f"send_task_success - taskToken={token}, output={token_response}")
    stfn.send_task_success(taskToken=token, output=token_response)

fl.server.start_server(config={"num_rounds": 3})
