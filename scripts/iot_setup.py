# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import logging
import json
import argparse


def read_cfg():
    client = boto3.client('cloudformation')

    with open('iot_setup.json', 'r') as F:
        cfg = json.load(F)

    try:
        response = client.describe_stacks(
            StackName=cfg['STACK_NAME'],
        )
        if 'Stacks' in response and response['Stacks'][0]['StackName'] == cfg['STACK_NAME']:
            for output in response['Stacks'][0]['Outputs']:
                cfg[output['OutputKey']] = output['OutputValue']
        logger.info(cfg)
        return cfg
    except Exception as e:
        logger.error(e)
        return None


def get_rules(cfg):
    client = boto3.client('iot')
    try:
        response = client.list_topic_rules()
        logger.info(f"Got {len(response['rules'])} rules")
        return response['rules']
    except Exception as e:
        logger.error(e)
        return []

def get_datastores(cfg):
    client = boto3.client('iotanalytics')
    try:
        response = client.list_datastores()
        logger.info(f"Got {len(response['datastoreSummaries'])} data stores")
        return response['datastoreSummaries']
    except Exception as e:
        logger.error(e)
        return []

def get_channels(cfg):
    client = boto3.client('iotanalytics')
    try:
        response = client.list_channels()
        logger.info(f"Got {len(response['channelSummaries'])} channels")
        return response['channelSummaries']
    except Exception as e:
        logger.error(e)
        return []

def get_pipelines(cfg):
    client = boto3.client('iotanalytics')
    try:
        response = client.list_pipelines()
        logger.info(f"Got {len(response['pipelineSummaries'])} pipelines")
        return response['pipelineSummaries']
    except Exception as e:
        logger.error(e)
        return []

def get_datasets(cfg):
    client = boto3.client('iotanalytics')
    try:
        response = client.list_datasets()
        logger.info(f"Got {len(response['datasetSummaries'])} data sets")
        return response['datasetSummaries']
    except Exception as e:
        logger.error(e)
        return []

def create_dataset(cfg, datastore):
    client = boto3.client('iotanalytics')
    try:
        logger.info(f"Creating dataset")
        response = client.create_dataset(
            datasetName=f"dataset_{cfg['DEF_UNIQUE_KEY']}",
            actions=[
                {
                    'actionName': f"dt_act_{cfg['DEF_UNIQUE_KEY']}",
                    'queryAction': {
                        'sqlQuery': f"SELECT DISTINCT client, MAX(time) FROM {datastore} WHERE time > to_unixtime(current_timestamp - interval '1' hour)  GROUP BY client ORDER BY MAX(time), client DESC LIMIT 10"
                    },
                },
            ],
            triggers=[
                {
                    'schedule': {
                        'expression': 'cron(0 * * * ? *)'
                    },
                },
            ],
            retentionPeriod={
                'unlimited': False,
                'numberOfDays': 30
            },
        )
        return response
    except Exception as e:
        logger.error(e)
        return None

def create_rule_for_analytics(cfg, name, sql, ch_name):
    client = boto3.client('iot')
    try:
        logger.info(f"Creating topic rule {name}")
        client.create_topic_rule(
            ruleName=f"rule_{name}_{cfg['DEF_UNIQUE_KEY']}",
            topicRulePayload={
                'sql': sql,
                'actions': [
                    {
                        'iotAnalytics': {
                            'channelName': ch_name,
                            'batchMode': False,
                            'roleArn': cfg['IotRoleArn']
                        },
                    }
                ],
                'ruleDisabled': False
            }
        )
    except Exception as e:
        logger.error(e)

def create_rule(cfg, name, sql, method):
    client = boto3.client('iot')
    try:
        logger.info(f"Creating topic rule {name}")
        client.create_topic_rule(
            ruleName=f"rule_{name}_{cfg['DEF_UNIQUE_KEY']}",
            topicRulePayload={
                'sql': sql,
                'actions': [
                    {
                        'dynamoDB': {
                            'tableName': cfg['TableName'],
                            'roleArn': cfg['IotRoleArn'],
                            'operation': 'INSERT',
                            'hashKeyField': 'client',
                            'hashKeyValue': '${client}',
                            'hashKeyType': 'STRING',
                            'rangeKeyField': 'type',
                            'rangeKeyValue': method,
                            'rangeKeyType': 'STRING',
                            'payloadField': 'path'
                        }
                    }
                ],
                'ruleDisabled': False
            }
        )
    except Exception as e:
        logger.error(e)

def create_datastore(cfg):
    client = boto3.client('iotanalytics')
    try:
        logger.info(f"Creating datastore")
        response = client.create_datastore(
            datastoreName=f"ds_{cfg['DEF_UNIQUE_KEY']}",
            datastoreStorage={
                'serviceManagedS3': {} ,
            },
            retentionPeriod={
                'unlimited': False,
                'numberOfDays': 30
            },
            fileFormatConfiguration={
                'jsonConfiguration': {}
            }
        )
        return response
    except Exception as e:
        logger.error(e)
        return None

def create_channel(cfg):
    client = boto3.client('iotanalytics')
    try:
        logger.info(f"Creating channel")
        response = client.create_channel(
            channelName=f"ch_{cfg['DEF_UNIQUE_KEY']}",
            channelStorage={
                'serviceManagedS3': {}
            },
            retentionPeriod={
                'unlimited': False,
                'numberOfDays': 30
            }
        )
        return response
    except Exception as e:
        logger.error(e)
        return None

def create_pipeline(cfg, ch, ds):
    client = boto3.client('iotanalytics')
    try:
        logger.info(f"Creating pipeline")
        response = client.create_pipeline(
            pipelineName=f"pl_{cfg['DEF_UNIQUE_KEY']}",
            pipelineActivities=[
                {
                    'channel': {
                        'name': 'channel_input',
                        'channelName': ch,
                        'next': 'datastore_output'
                    },
                },
                {
                    'datastore': {
                        'name': 'datastore_output',
                        'datastoreName': ds
                    }
                }
            ]
        )
        return response
    except Exception as e:
        logger.error(e)
        return None


def create_ssm_param(cfg, value):
    client = boto3.client('ssm')
    try:
        logger.info(f"creating ssm param")
        response = client.put_parameter(
            Name=cfg['DatasetParam'],
            Value=value,
            Type='String',
            Overwrite=True,
            DataType='text'
        )
        return response
    except Exception as e:
        logger.error(e)
        return None


def delete_ssm_param(cfg):
    client = boto3.client('ssm')
    try:
        logger.info(f"deleting ssm param")
        response = client.delete_parameter(
            Name=cfg['DatasetParam']
        )
        return response
    except Exception as e:
        logger.error(e)
        return None


def delete_dataset(cfg):
    client = boto3.client('iotanalytics')
    try:
        logger.info(f"Deleting dataset")
        response = client.delete_dataset(
            datasetName=f"dataset_{cfg['DEF_UNIQUE_KEY']}"
        )
        return response
    except Exception as e:
        logger.error(e)
        return None


def delete_rule(cfg, name):
    client = boto3.client('iot')
    try:
        logger.info(f"Deleting topic rule {name}")
        response = client.delete_topic_rule(
            ruleName=f"rule_{name}_{cfg['DEF_UNIQUE_KEY']}",
        )
        return response
    except Exception as e:
        logger.error(e)
        return None


def delete_channel(cfg):
    client = boto3.client('iotanalytics')
    try:
        logger.info(f"Deleting channel")
        response = client.delete_channel(
            channelName=f"ch_{cfg['DEF_UNIQUE_KEY']}"
        )
        return response
    except Exception as e:
        logger.error(e)
        return None


def delete_pipeline(cfg):
    client = boto3.client('iotanalytics')
    try:
        logger.info(f"deleting pipeline")
        response = client.delete_pipeline(
            pipelineName=f"pl_{cfg['DEF_UNIQUE_KEY']}"
        )
        return response
    except Exception as e:
        logger.error(e)
        return None


def delete_datastore(cfg):
    client = boto3.client('iotanalytics')
    try:
        logger.info(f"Deleting datastore")
        response = client.delete_datastore(
            datastoreName=f"ds_{cfg['DEF_UNIQUE_KEY']}"
        )
        return response
    except Exception as e:
        logger.error(e)
        return None

def iot_setup(cfg):
    rules_to_make = [ ('get', "SELECT * FROM 'parameters/client/+/sent'", 'get'),
        ('set', "SELECT * FROM 'set/client/+/sent'", 'set'),
        ('fit', "SELECT * FROM 'fit/client/+/sent'", 'fit'),
        ('evaluate', "SELECT * FROM 'evaluate/client/+/sent'", 'evaluate')]
    rules = get_rules(cfg)
    for rule_name, sql, method in rules_to_make:
        exists = False
        for r in rules:
            if cfg['DEF_UNIQUE_KEY'] in r['ruleName'] and rule_name in r['ruleName']:
                logger.info(f"Rule {rule_name} already exists")
                exists = True
                break
        if exists == False:
            create_rule(cfg, rule_name, sql, method)

    # data store
    ds = get_datastores(cfg)
    exists = False
    for d in ds:
        if cfg['DEF_UNIQUE_KEY'] in d['datastoreName']:
            logger.info(f"Datastore already exists")
            exists = True
            datastore = d
            break
    if exists == False:
        datastore = create_datastore(cfg)

    # channel
    ch = get_channels(cfg)
    exists = False
    for c in ch:
        if cfg['DEF_UNIQUE_KEY'] in c['channelName']:
            logger.info(f"Channel already exists")
            exists = True
            channel = c
            break
    if exists == False:
        channel = create_channel(cfg)

    # pipeline
    pl = get_pipelines(cfg)
    exists = False
    for p in pl:
        if cfg['DEF_UNIQUE_KEY'] in p['pipelineName']:
            logger.info(f"Pipeline already exists")
            exists = True
            break
    if exists == False:
        create_pipeline(cfg, channel['channelName'], datastore['datastoreName'])

    # rule
    rules_to_make = [ ('heartbeat', "SELECT * FROM 'flower/clients/#'", channel['channelName'])]
    for rule_name, sql, ch in rules_to_make:
        exists = False
        for r in rules:
            if cfg['DEF_UNIQUE_KEY'] in r['ruleName'] and rule_name in r['ruleName']:
                logger.info(f"Rule {rule_name} already exists")
                exists = True
                break
        if exists == False:
            create_rule_for_analytics(cfg, rule_name, sql, ch)

    # data set
    datasets = get_datasets(cfg)
    exists = False
    for dt in datasets:
        if cfg['DEF_UNIQUE_KEY'] in dt['datasetName']:
            logger.info(f"Dataset already exists")
            dataset = dt
            exists = True
            break
    if exists == False:
        dataset = create_dataset(cfg, datastore['datastoreName'])

    create_ssm_param(cfg, dataset['datasetName'])


def iot_cleanup(cfg):
    delete_dataset(cfg)

    rules_to_delete = ['heartbeat', 'get', 'set', 'fit', 'evaluate']
    for rule_name in rules_to_delete:
        delete_rule(cfg, rule_name)

    delete_pipeline(cfg)
    delete_channel(cfg)
    delete_datastore(cfg)

    delete_ssm_param(cfg)

if __name__ == "__main__":

    # Configure logging
    logging.getLogger('').setLevel(logging.WARN)
    logger = logging.getLogger('IotSetup')
    logger.setLevel(logging.INFO)
    logger_ch = logging.StreamHandler()
    logger_ch.setLevel(logging.INFO)
    logger_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
    logger_ch.setFormatter(logger_formatter)
    logger.addHandler(logger_ch)

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--clean", action="store_true",
                        help="cleanup the iot configuration")
    args = parser.parse_args()

    cfg = read_cfg()
    logger.info(f"Found configuration data: {cfg}")

    if args.clean:
        logger.info("deleting iot configuration")
        iot_cleanup(cfg)
    else:
        logger.info("creating iot configuration")
        iot_setup(cfg)


