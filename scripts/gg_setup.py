# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import logging
import json
import traceback

def read_cfg():
    with open('gg_setup.json', 'r') as F:
        cfg = json.load(F)
        return cfg

def register_role(cfg):
    client = boto3.client('greengrass')
    exists = True
    try:
        response = client.get_associated_role(
            GroupId=cfg['GG_GROUP']
        )
        if response['RoleArn'] == cfg['GG_ROLE_ARN']:
            logger.info(f"Role {cfg['GG_ROLE_ARN']} already registered")
        else:
            exists = False
    except Exception as e:
        logger.info(f"Role {cfg['GG_ROLE_ARN']} already registered")
        exists = False

    if exists == False:
        logger.info(f"Registering role {cfg['GG_ROLE_ARN']} to group {cfg['GG_GROUP']}")
        try:
            response = client.associate_role_to_group(
                GroupId=cfg['GG_GROUP'],
                RoleArn=cfg['GG_ROLE_ARN']
            )
        except Exception as e:
            logger.error(e)

def get_latest_group_version(cfg):
    client = boto3.client('greengrass')
    try:
        response = client.get_group(
            GroupId=cfg['GG_GROUP'],
        )
        latest_version_arn = response['LatestVersionArn']
        latest_version_id = response['LatestVersion']
        logger.info(f"Latest group version: {latest_version_id} - {latest_version_arn}")
        return latest_version_arn, latest_version_id
    except Exception as e:
        logger.info(f"Group {cfg['GG_GROUP']} has no version yet")
        return None, None

def get_group_definition(cfg, version_arn):
    client = boto3.client('greengrass')
    try:
        response = client.get_group_version(
            GroupId=cfg['GG_GROUP'],
            GroupVersionId=version_arn
        )
        logger.info(f"Got group version info for group {cfg['GG_GROUP']}")
        return response['Definition']
    except Exception as e:
        logger.error(e)
        return []

def get_resource_definitions(cfg):
    client = boto3.client('greengrass')
    try:
        response = client.list_resource_definitions()
        logger.info(f"Got {len(response['Definitions'])} resource definitions")
        return response['Definitions']
    except Exception as e:
        logger.error(e)
        return []

def get_fn_definitions(cfg):
    client = boto3.client('greengrass')
    try:
        response = client.list_function_definitions()
        logger.info(f"Got {len(response['Definitions'])} fn definitions")
        return response['Definitions']
    except Exception as e:
        logger.error(e)
        return []

def get_sub_definitions(cfg):
    client = boto3.client('greengrass')
    try:
        response = client.list_subscription_definitions()
        logger.info(f"Got {len(response['Definitions'])} subscription definitions")
        return response['Definitions']
    except Exception as e:
        logger.error(e)
        return []

def get_log_definitions(cfg):
    client = boto3.client('greengrass')
    try:
        response = client.list_logger_definitions()
        logger.info(f"Got {len(response['Definitions'])} logger definitions")
        return response['Definitions']
    except Exception as e:
        logger.error(e)
        return []


def create_log_def(cfg):
    client = boto3.client('greengrass')
    try:
        response = client.create_logger_definition(
            Name=f"logdef-{cfg['DEF_UNIQUE_KEY']}",
            InitialVersion={
                'Loggers': [
                    {
                        'Component': 'GreengrassSystem',
                        'Id': f"GreengrassSystemFS-{cfg['DEF_UNIQUE_KEY']}",
                        'Type': 'FileSystem',
                        'Level': 'INFO',
                        'Space': 25600
                    },
                    {
                        'Component': 'Lambda',
                        'Id': f"LambdaFS-{cfg['DEF_UNIQUE_KEY']}",
                        'Type': 'FileSystem',
                        'Level': 'INFO',
                        'Space': 25600
                    },
                    {
                        'Component': 'GreengrassSystem',
                        'Id': f"GreengrassSystemCW-{cfg['DEF_UNIQUE_KEY']}",
                        'Type': 'AWSCloudWatch',
                        'Level': 'INFO'
                    },
                    {
                        'Component': 'Lambda',
                        'Id': f"LambdaCW-{cfg['DEF_UNIQUE_KEY']}",
                        'Type': 'AWSCloudWatch',
                        'Level': 'INFO'
                    }
                ]
            }
        )
        logger.info(response)
        return response
    except Exception as e:
        logger.error(e)
        return []


def create_fs_resource(cfg):
    client = boto3.client('greengrass')
    try:
        logger.info(f"Creating file system resource")
        response = client.create_resource_definition(
            InitialVersion={
                'Resources': [
                    {
                        'Id': f"tmpdir-{cfg['DEF_UNIQUE_KEY']}",
                        'Name': f"tmpdir-{cfg['DEF_UNIQUE_KEY']}",
                        'ResourceDataContainer': {
                            'LocalVolumeResourceData': {
                                'DestinationPath': '/data',
                                'SourcePath': '/tmp'
                            },
                        }
                    },
                ]
            },
            Name=f"rd-{cfg['DEF_UNIQUE_KEY']}",
        )
        return response
    except Exception as e:
        logger.error(e)
        return None

def create_fn(cfg):
    client = boto3.client('greengrass')
    try:
        logger.info(f"Creating lambda fn")
        response = client.create_function_definition(
            InitialVersion={
                'DefaultConfig': {
                    'Execution': {
                        'IsolationMode': 'GreengrassContainer'
                    }
                },
                'Functions': [
                    {
                        'FunctionArn': cfg['FN_ARN'],
                        'FunctionConfiguration': {
                            'EncodingType': 'json',
                            'Environment': {
                                'AccessSysfs': False,
                                'Execution': {
                                    'IsolationMode': 'GreengrassContainer'
                                },
                                'ResourceAccessPolicies': [
                                    {
                                        'Permission': 'rw',
                                        'ResourceId': f"tmpdir-{cfg['DEF_UNIQUE_KEY']}",
                                    },
                                ],
                            },
                            'MemorySize': 4194304,
                            'Pinned': True,
                            'Timeout': 900
                        },
                        'Id': f"fn-{cfg['DEF_UNIQUE_KEY']}",
                    },
                    {
                        "FunctionArn": "arn:aws:lambda:::function:GGStreamManager:1",
                        "FunctionConfiguration": {
                            "Environment": {
                                "Variables": {
                                    "STREAM_MANAGER_READ_ONLY_DIRS": "/tmp"
                                }
                            },
                            "MemorySize": 4194304,
                            "Pinned": True,
                            "Timeout": 3
                        },
                        "Id": "streamManager"
                    }
                ]
            },
            Name=f"fd-{cfg['DEF_UNIQUE_KEY']}",
        )
        return response
    except Exception as e:
        logger.error(e)
        return None

def create_sub_defs(cfg, subs_to_make):
    client = boto3.client('greengrass')
    subs = []
    for name, src, subject, tgt in subs_to_make:
        subs.append(
            {
                'Id': f"sub-{cfg['DEF_UNIQUE_KEY']}-{name}",
                'Source': src,
                'Subject': subject,
                'Target': tgt
            }
        )
    try:
        logger.info(f"Creating subscription {name}")
        response = client.create_subscription_definition(
            InitialVersion={
                'Subscriptions': subs
            },
            Name=f"sub-{cfg['DEF_UNIQUE_KEY']}-{name}"
        )
        return response
    except Exception as e:
        logger.error(e)
        return None
    
def create_group_version(cfg, rd_arn, fn_arn, sub_arn, log_arn, core_arn):
    client = boto3.client('greengrass')
    logger.info(f"Creating new group version")
    try:
        response = client.create_group_version(
            CoreDefinitionVersionArn=core_arn,
            FunctionDefinitionVersionArn=fn_arn,
            GroupId=cfg['GG_GROUP'],
            ResourceDefinitionVersionArn=rd_arn,
            SubscriptionDefinitionVersionArn=sub_arn,
            LoggerDefinitionVersionArn=log_arn
        )
        return response
    except Exception as e:
        logger.error(e)
        return None

if __name__ == "__main__":

    # Configure logging
    logging.getLogger('').setLevel(logging.WARN)
    logger = logging.getLogger('GreenGrassSetup')
    logger.setLevel(logging.INFO)
    logger_ch = logging.StreamHandler()
    logger_ch.setLevel(logging.INFO)
    logger_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
    logger_ch.setFormatter(logger_formatter)
    logger.addHandler(logger_ch)

    cfg = read_cfg()
    logger.info("Read configuration data")

    register_role(cfg)

    resource_defs = get_resource_definitions(cfg)
    exists = False
    for r in resource_defs:
        if cfg['DEF_UNIQUE_KEY'] in r['Name']:
            logger.info("FS resource already exists")
            fs_resource = r
            exists = True
            break
    if exists == False:
        fs_resource = create_fs_resource(cfg )

    fn_defs = get_fn_definitions(cfg)
    exists = False
    for f in fn_defs:
        if cfg['DEF_UNIQUE_KEY'] in f['Name']:
            logger.info("Fn already exists")
            fn_resource = f
            exists = True
            break
    if exists == False:
        fn_resource = create_fn(cfg )

    sub_defs = get_sub_definitions(cfg)
    subs_to_make = [('fit', cfg['FN_ARN'], 'fit/client/+/sent', 'cloud'), 
        ('evaluate', cfg['FN_ARN'], 'evaluate/client/+/sent','cloud'), 
        ('parameters', cfg['FN_ARN'], 'parameters/client/+/sent','cloud'), 
        ('set', cfg['FN_ARN'], 'set/client/+/sent','cloud'), 
        ('commands', 'cloud', 'commands/client/+/update', cfg['FN_ARN']), 
        ('heartbeat', cfg['FN_ARN'], 'flower/clients/#','cloud')]
    exists = False
    for s in sub_defs:
        if cfg['DEF_UNIQUE_KEY'] in s['Name']:
            logger.info(f"Subscription already exists")
            sub_resource = s
            exists = True
            break
    if exists == False:
        sub_resource = create_sub_defs(cfg, subs_to_make)

    exists = False
    log_defs = get_log_definitions(cfg)
    for log_def in log_defs:
        if 'Name' in log_def and cfg['DEF_UNIQUE_KEY'] in log_def['Name']:
            log_def_resource = log_def
            exists = True
            break
    if not exists:
        log_def_resource = create_log_def(cfg)

    group_version_arn, group_version_id = get_latest_group_version(cfg)
    group_def = get_group_definition(cfg, group_version_id)

    gv = create_group_version(cfg, fs_resource['LatestVersionArn'], fn_resource['LatestVersionArn'], sub_resource['LatestVersionArn'], log_def['LatestVersionArn'], group_def['CoreDefinitionVersionArn'])

    logger.info("Starting deployment")
    client = boto3.client('greengrass')
    try:
        response = client.create_deployment(
            DeploymentType='NewDeployment',
            GroupId=cfg['GG_GROUP'],
            GroupVersionId=gv['Version']
        )
    except Exception as e:
        logger.error(e)
