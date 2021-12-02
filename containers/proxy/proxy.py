# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from collections import OrderedDict
import json
import time
import tempfile
import os
import pickle
import logging
import sys

import flwr as fl

import boto3

# Setup logging to stdout
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# Read environment data
client_id = os.environ['CLIENT']
region = os.environ['AWS_REGION']
endpoint_url = os.environ['ENDPOINT']
coordinator_url = os.environ['COORDINATOR']
tbl = os.environ['TABLE']
bucket = os.environ['BUCKET']

# set up clients
iot = boto3.client('iot-data', region_name=region, endpoint_url=endpoint_url)
ddbclient = boto3.client('dynamodb', region_name=region)
s3 = boto3.client('s3', region_name=region, verify=False)

class CifarClient(fl.client.NumPyClient):
    def get_parameters(self):
        logger.info("Starting get_parameters")
        payload = {
            "method": "get_parameters",
            "bucket": bucket,
            "prefix": f"parameters/get/{client_id}/params.pkl"
        }
        response = iot.publish(
            topic=f"commands/client/{client_id}/update",
            qos=0,
            payload=json.dumps(payload)
        )
        logger.info(f"Sent command for get_parameters: {json.dumps(payload)}")

        found = False
        while found == False:
            response = ddbclient.query( TableName=tbl, Select = 'ALL_ATTRIBUTES', 
                            KeyConditionExpression = "client = :client AND #attmethod = :method",
                            ExpressionAttributeNames =  { "#attmethod": "type" },
                            ExpressionAttributeValues = { ":client":{"S":client_id}, ":method":{"S":"get"}},
                        )
            for i in response['Items']:
                found = True
                # "path":{"M":{"path":{"S":"s3://rd-flower/test/p2.pkl"},"client":{"S":"client1"}}}
                path = i['path']['M']
                logger.info(f"Got response for get_parameters: {json.dumps(path)}")
                s3_path = path['path']['S']
                s3_parts = s3_path.split('/')
                n_bucket = s3_parts[2]
                n_prefix = '/'.join(s3_parts[3:])
                temp_file_d, temp_file_path = tempfile.mkstemp(dir='/tmp')
                os.close(temp_file_d)
                s3.download_file(n_bucket, n_prefix, temp_file_path)
                with open(temp_file_path, 'rb') as pkl_file:
                    p = pickle.load(pkl_file)
                    ddbclient.delete_item(Key={'client': {'S': client_id}, 'type': {'S': 'get'}}, TableName=tbl)
                    return p
            logger.info("Response for get_params not found yet")
            time.sleep(30)

    def set_parameters(self, parameters):
        logger.info("Starting set_parameters")
        prefix = f"parameters/set/{client_id}/params.pkl"
        with tempfile.NamedTemporaryFile() as fp:
            pickle.dump(parameters, fp)
            logger.info(f"Dumped parameters to : {fp.name}")
            s3.upload_file(fp.name, bucket, prefix)
        payload = {
            "method": "set_parameters",
            "bucket": bucket,
            "prefix": prefix
        }
        response = iot.publish(
            topic=f'commands/client/{client_id}/update',
            qos=0,
            payload=json.dumps(payload)
        )
        logger.info(f"Sent command for set_parameters: {json.dumps(payload)}")

        found = False
        while found == False:
            response = ddbclient.query( TableName=tbl, Select = 'ALL_ATTRIBUTES', 
                            KeyConditionExpression = "client = :client AND #attmethod = :method",
                            ExpressionAttributeNames =  { "#attmethod": "type" },
                            ExpressionAttributeValues = { ":client":{"S":client_id}, ":method":{"S":"set"} },
                        )
            for i in response['Items']:
                found = True
                logger.info(f"Got response for set_parameters: {json.dumps(i)}")
                ddbclient.delete_item(Key={'client': {'S': client_id}, 'type': {'S': 'set'}}, TableName=tbl)
                break
            logger.info("Response for set_params not found yet")
            time.sleep(30)

    def fit(self, parameters, config):
        logger.info("Starting fit")
        prefix = f"parameters/set/{client_id}/fit.pkl"
        out_prefix = f"parameters/get/{client_id}/fit.pkl"
        with tempfile.NamedTemporaryFile() as fp:
            pickle.dump(parameters, fp)
            logger.info(f"Dumped parameters to : {fp.name}")
            s3.upload_file(fp.name, bucket, prefix)
        payload = {
            "method": "fit",
            "bucket": bucket,
            "prefix": prefix,
            "out_bucket": bucket,
            "out_prefix": out_prefix
        }
        response = iot.publish(
            topic=f'commands/client/{client_id}/update',
            qos=0,
            payload=json.dumps(payload)
        )
        logger.info(f"Sent command for fit: {json.dumps(payload)}")

        found = False
        while found == False:
            response = ddbclient.query( TableName=tbl, Select = 'ALL_ATTRIBUTES', 
                            KeyConditionExpression = "client = :client AND #attmethod = :method",
                            ExpressionAttributeNames =  { "#attmethod": "type" },
                            ExpressionAttributeValues = { ":client":{"S":client_id}, ":method":{"S":"fit"} },
                        )
            for i in response['Items']:
                found = True
                msg = i['path']['M']
                logger.info(f"Got response for fit: {json.dumps(msg)}")
                s3_path = msg['path']['S']
                train_len = msg['train_len']['N']
                d = msg['dict']['M']
                s3_parts = s3_path.split('/')
                n_bucket = s3_parts[2]
                n_prefix = '/'.join(s3_parts[3:])
                temp_file_d, temp_file_path = tempfile.mkstemp(dir='/tmp')
                os.close(temp_file_d)
                s3.download_file(n_bucket, n_prefix, temp_file_path)
                with open(temp_file_path, 'rb') as pkl_file:
                    p = pickle.load(pkl_file)
                    ddbclient.delete_item(Key={'client': {'S': client_id}, 'type': {'S': 'fit'}}, TableName=tbl)
                    return p, int(train_len), d
            logger.info("Response for fit not found yet")
            time.sleep(30)


    def evaluate(self, parameters, config):
        logger.info("Starting evaluate")
        prefix = f"parameters/set/{client_id}/eval.pkl"
        with tempfile.NamedTemporaryFile() as fp:
            pickle.dump(parameters, fp)
            logger.info(f"Dumped parameters to : {fp.name}")
            s3.upload_file(fp.name, bucket, prefix)
        payload = {
            "method": "evaluate",
            "bucket": bucket,
            "prefix": prefix
        }
        response = iot.publish(
            topic=f'commands/client/{client_id}/update',
            qos=0,
            payload=json.dumps(payload)
        )
        logger.info(f"Sent command for evaluate: {json.dumps(payload)}")

        found = False
        while found == False:
            response = ddbclient.query( TableName=tbl, Select = 'ALL_ATTRIBUTES', 
                            KeyConditionExpression = "client = :client AND #attmethod = :method",
                            ExpressionAttributeNames =  { "#attmethod": "type" },
                            ExpressionAttributeValues = { ":client":{"S":client_id}, ":method":{"S":"evaluate"} },
                        )
            for i in response['Items']:
                found = True
                msg = i['path']['M']
                logger.info(f"Got response for evaluate: {json.dumps(msg)}")
                test_len = msg['train_len']['N']
                loss = msg['loss']['N']
                accuracy = {
                    'accuracy': float(msg['accuracy']['M']['accuracy']['N'])
                }

                # For constructing a CloudWatch log metric filter, we publish a clean JSON representation.
                cw_msg = {
                    "client": client_id,
                    "loss": loss,
                    "accuracy": float(msg['accuracy']['M']['accuracy']['N'])
                }
                print(json.dumps(cw_msg))

                # msg['accuracy']['M']
                ddbclient.delete_item(Key={'client': {'S': client_id}, 'type': {'S': 'evaluate'}}, TableName=tbl)
                return float(loss), int(test_len), accuracy
            logger.info("Response for evaluate not found yet")
            time.sleep(30)

fl.client.start_numpy_client(coordinator_url, client=CifarClient())
