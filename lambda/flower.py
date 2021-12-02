# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
This Lambda function runs indefinitely on a GreenGrass core device.  

In the main processing part, it loads a canned model and data set.

When it receives an incoming message, it parses for the command.

Supported commands:

* get_parameters
    * bucket
    * prefix
* set_parameters
    * bucket
    * prefix
* fit
    * bucket
    * prefix
    * out_bucket
    * out_prefix
* evaluate 
    * bucket
    * prefix

MQTT topics used:

* flower/clients/<client id> for heartbeats
* commands/client/<client id>/update to send commands to the GG core
* parameters/client/<client id>/sent to send response of `get_parameters`
* set/client/<client id>/sent to send response of `set_parameters`
* fit/client/<client id>/sent to send response of `fit`
* evaluate/client/<client id>/sent to send response of `evaluate`

MQTT example events:

    {
        "method": "fit",
        "bucket": "rd-flower",
        "prefix": "test/params.pkl",
        "out_bucket": "rd-flower",
        "out_prefix": "test/new_params.pkl"
    }
"""

#
# Imports 
#
import logging
import platform
import sys
from threading import Timer
import json
from collections import OrderedDict
import asyncio
import time
import pickle
import tempfile
import uuid
import traceback
import os

import greengrasssdk
from greengrasssdk.stream_manager import (
    ExportDefinition,
    MessageStreamDefinition,
    ReadMessagesOptions,
    ResourceNotFoundException,
    S3ExportTaskDefinition,
    S3ExportTaskExecutorConfig,
    Status,
    StatusConfig,
    StatusLevel,
    StatusMessage,
    StrategyOnFull,
    StreamManagerClient,
    StreamManagerException,
    NotEnoughMessagesException
)
from greengrasssdk.stream_manager.util import Util
import boto3

import torch
import torch.nn as nn
import torch.nn.functional as F
import torchvision.transforms as transforms
from torch.utils.data import DataLoader
from torchvision.datasets import CIFAR10

import flwr as fl

#
# PyTorch related methods
#
DEVICE = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
def load_data():
    """Load CIFAR-10 (training and test set)."""
    transform = transforms.Compose(
    [transforms.ToTensor(), transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))]
    )
    trainset = CIFAR10("/data", train=True, download=True, transform=transform)
    testset = CIFAR10("/data", train=False, download=True, transform=transform)
    trainloader = DataLoader(trainset, batch_size=32, shuffle=True)
    testloader = DataLoader(testset, batch_size=32)
    return trainloader, testloader

def train(net, trainloader, epochs):
    """Train the network on the training set."""
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(net.parameters(), lr=0.001, momentum=0.9)
    for _ in range(epochs):
        for images, labels in trainloader:
            images, labels = images.to(DEVICE), labels.to(DEVICE)
            optimizer.zero_grad()
            loss = criterion(net(images), labels)
            loss.backward()
            optimizer.step()

def test(net, testloader):
    """Validate the network on the entire test set."""
    criterion = torch.nn.CrossEntropyLoss()
    correct, total, loss = 0, 0, 0.0
    with torch.no_grad():
        for data in testloader:
            images, labels = data[0].to(DEVICE), data[1].to(DEVICE)
            outputs = net(images)
            loss += criterion(outputs, labels).item()
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum().item()
    accuracy = correct / total
    return loss, accuracy

class Net(nn.Module):
    def __init__(self) -> None:
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(3, 6, 5)
        self.pool = nn.MaxPool2d(2, 2)
        self.conv2 = nn.Conv2d(6, 16, 5)
        self.fc1 = nn.Linear(16 * 5 * 5, 120)
        self.fc2 = nn.Linear(120, 84)
        self.fc3 = nn.Linear(84, 10)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.pool(F.relu(self.conv1(x)))
        x = self.pool(F.relu(self.conv2(x)))
        x = x.view(-1, 16 * 5 * 5)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x

#
# Static init
#

# Setup logging to stdout
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# Load model and data
net = Net().to(DEVICE)
trainloader, testloader = load_data()
logger.info("Downloaded dataset")

# Creating a greengrass core sdk client
client = greengrasssdk.client("iot-data")
logger.info("Created GG client")

# Retrieving platform information to send from Greengrass Core
my_platform = platform.platform()

#client_id = 'client1'
client_id = str(uuid.uuid4())

#
# Heartbeat
#
def greengrass_hello_world_run():
    try:
        logger.info("Publishing message")
        payload = {
            "client": client_id,
            "time": int(time.time())
        }
        client.publish(topic="flower/clients/{0}".format(client_id), payload=json.dumps(payload))
    except Exception as e:
        logger.error("Failed to publish message: " + repr(e))
        traceback.print_exc()

    # Asynchronously schedule this function to be run again in 60 seconds
    Timer(60, greengrass_hello_world_run).start()

# Start executing the function above
greengrass_hello_world_run()

#
# Flower methods
#
def get_parameters():
    return [val.cpu().numpy() for _, val in net.state_dict().items()]

def set_parameters(parameters):
    params_dict = zip(net.state_dict().keys(), parameters)
    state_dict = OrderedDict({k: torch.Tensor(v) for k, v in params_dict})
    net.load_state_dict(state_dict, strict=True)

def fit(parameters):
    set_parameters(parameters)
    train(net, trainloader, epochs=1)
    return get_parameters(), len(trainloader), {}

def evaluate(parameters):
    set_parameters(parameters)
    loss, accuracy = test(net, testloader)
    return float(loss), len(testloader), {"accuracy":float(accuracy)}

# 
# IoT helper methods
#
def get_input_topic(context):
    try:
        topic = context.client_context.custom['subject']
    except Exception as e:
        logger.error('Topic could not be parsed. ' + repr(e))
        traceback.print_exc()
    return topic
    
def upload_to_s3(bucket, prefix, fpath):
    try:
        stream_name = f"S3UploadStream{int(time.time())}"
        status_stream_name = f"{stream_name}Status"
        bucket_name = bucket
        key_name = prefix
        file_url = f"file://{fpath}"
        s_client = StreamManagerClient()
        logger.info(f"Uploading to S3 from: {file_url}")

        # Try deleting the status stream (if it exists) so that we have a fresh start
        try:
            s_client.delete_message_stream(stream_name=status_stream_name)
        except ResourceNotFoundException:
            pass

        # Try deleting the stream (if it exists) so that we have a fresh start
        try:
            s_client.delete_message_stream(stream_name=stream_name)
        except ResourceNotFoundException:
            pass

        exports = ExportDefinition(
            s3_task_executor=[
                S3ExportTaskExecutorConfig(
                    identifier="S3TaskExecutor" + stream_name,  # Required
                    # Optional. Add an export status stream to add statuses for all S3 upload tasks.
                    status_config=StatusConfig(
                        status_level=StatusLevel.INFO,  # Default is INFO level statuses.
                        # Status Stream should be created before specifying in S3 Export Config.
                        status_stream_name=status_stream_name,
                    ),
                )
            ]
        )

        # Create the Status Stream.
        s_client.create_message_stream(
            MessageStreamDefinition(name=status_stream_name, strategy_on_full=StrategyOnFull.OverwriteOldestData)
        )

        # Create the message stream with the S3 Export definition.
        s_client.create_message_stream(
            MessageStreamDefinition(
                name=stream_name, strategy_on_full=StrategyOnFull.OverwriteOldestData, export_definition=exports
            )
        )

        # Append a S3 Task definition and print the sequence number
        s3_export_task_definition = S3ExportTaskDefinition(input_url=file_url, bucket=bucket_name, key=key_name)
        logger.info(
            "Successfully appended S3 Task Definition to stream with sequence number %d",
            s_client.append_message(stream_name, Util.validate_and_serialize_to_json_bytes(s3_export_task_definition)),
        )

        # Read the statuses from the export status stream
        is_file_uploaded_to_s3 = False
        while not is_file_uploaded_to_s3:
            try:
                messages_list = s_client.read_messages(
                    status_stream_name, ReadMessagesOptions(min_message_count=1, read_timeout_millis=10000)
                )
                for message in messages_list:
                    # Deserialize the status message first.
                    status_message = Util.deserialize_json_bytes_to_obj(message.payload, StatusMessage)

                    # Check the status of the status message. If the status is "Success",
                    # the file was successfully uploaded to S3.
                    # If the status was either "Failure" or "Cancelled", the server was unable to upload the file to S3.
                    # We will print the message for why the upload to S3 failed from the status message.
                    # If the status was "InProgress", the status indicates that the server has started uploading
                    # the S3 task.
                    if status_message.status == Status.Success:
                        logger.info("Successfully uploaded file at path " + file_url + " to S3.")
                        is_file_uploaded_to_s3 = True
                    elif status_message.status == Status.Failure or status_message.status == Status.Canceled:
                        logger.info(
                            "Unable to upload file at path " + file_url + " to S3. Message: " + status_message.message
                        )
                        is_file_uploaded_to_s3 = True
                time.sleep(5)
            except NotEnoughMessagesException:
                logger.info("NotEnoughMessagesException")
            except StreamManagerException:
                logger.exception("Exception while running")
    except asyncio.TimeoutError:
        logger.exception("Timed out while executing")
        traceback.print_exc()
    except Exception:
        logger.exception("Exception while running")
        traceback.print_exc()
    finally:
        if s_client:
            s_client.close()

"""
Invoked for new MQTT messages.
"""
def function_handler(event, context):
    try:
        input_topic = get_input_topic(context)

        # Make sure message is for this client
        # commands/client/client1/update
        topic_parts = input_topic.split('/')
        if topic_parts[2] == client_id:
            response = 'Invoked on topic "%s" with message "%s"' % (input_topic, json.dumps(event))
            logger.info(response)
        else:
            return

        method = event['method']
        if method == 'get_parameters':
            logger.info("Handling get_parameters")
            bucket = event['bucket']
            prefix = event['prefix']
            logger.info(f"Handling get_parameters: bucket = {bucket}, prefix = {prefix}")
            p = get_parameters()
            with tempfile.NamedTemporaryFile(dir = '/data', delete=False) as fp:
                pickle.dump(p, fp)
                logger.info(f"Dumped parameters to : {fp.name}")
                upload_to_s3(bucket, prefix, fp.name.replace('data', 'tmp'))
                payload = {
                    "client": client_id,
                    "path": f"s3://{bucket}/{prefix}"
                }
                client.publish(topic="parameters/client/{0}/sent".format(client_id), payload=json.dumps(payload))
        elif method == 'set_parameters':
            logger.info("Handling set_parameters")
            bucket = event['bucket']
            prefix = event['prefix']
            logger.info(f"Handling set_parameters: bucket = {bucket}, prefix = {prefix}")
            s3 = boto3.client('s3', region_name='us-west-2')
            temp_file_d, temp_file_path = tempfile.mkstemp(dir='/data')
            os.close(temp_file_d)
            s3.download_file(bucket, prefix, temp_file_path)
            with open(temp_file_path, 'rb') as pkl_file:
                p = pickle.load(pkl_file)
                set_parameters(p)
            logger.info(f"Updated parameters, type = {type(p)}, shape = {len(p)}")
            payload = {
                "client": client_id
            }
            client.publish(topic="set/client/{0}/sent".format(client_id), payload=json.dumps(payload))

        elif method == 'fit':
            logger.info("Handling fit")
            bucket = event['bucket']
            prefix = event['prefix']
            out_bucket = event['out_bucket']
            out_prefix = event['out_prefix']
            logger.info(f"Handling fit: bucket = {bucket}, prefix = {prefix}")
            s3 = boto3.client('s3', region_name='us-west-2')
            temp_file_d, temp_file_path = tempfile.mkstemp(dir='/data')
            os.close(temp_file_d)
            s3.download_file(bucket, prefix, temp_file_path)
            with open(temp_file_path, 'rb') as pkl_file:
                p = pickle.load(pkl_file)
                new_p, l, j = fit(p)
            logger.info(f"Ran fit: len trainloader = {l}")
            with tempfile.NamedTemporaryFile(dir = '/data', delete=False) as fp:
                pickle.dump(new_p, fp)
                logger.info(f"Dumped fit parameters to : {fp.name}")
                upload_to_s3(out_bucket, out_prefix, fp.name.replace('data', 'tmp'))
                payload = {
                    "client": client_id,
                    "path": f"s3://{out_bucket}/{out_prefix}",
                    "train_len": l,
                    "dict": j
                }
                client.publish(topic="fit/client/{0}/sent".format(client_id), payload=json.dumps(payload))
                logger.info("Done with fit")
        elif method == 'evaluate':
            logger.info("Handling evaluate")
            bucket = event['bucket']
            prefix = event['prefix']
            logger.info(f"Handling evaluate: bucket = {bucket}, prefix = {prefix}")
            s3 = boto3.client('s3', region_name='us-west-2')
            temp_file_d, temp_file_path = tempfile.mkstemp(dir='/data')
            os.close(temp_file_d)
            s3.download_file(bucket, prefix, temp_file_path)
            with open(temp_file_path, 'rb') as pkl_file:
                p = pickle.load(pkl_file)
                loss, l, accuracy = evaluate(p)
            logger.info(f"Ran evaluate: len trainloader = {l}")
            payload = {
                "client": client_id,
                "loss": loss,
                "train_len": l,
                "accuracy": accuracy
            }
            client.publish(topic="evaluate/client/{0}/sent".format(client_id), payload=json.dumps(payload))
            logger.info("Done with evaluate")
        else:
            logger.warn(f"Invalid method: {method}")
    except Exception as e:
        logger.error(e)
        traceback.print_exc()
