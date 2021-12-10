# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import logging
import json
import argparse


def read_cfg():
    client = boto3.client('cloudformation')

    with open('gg_setup.json', 'r') as F:
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


def register_role(cfg, gg_group):
    client = boto3.client('greengrass')
    exists = True
    try:
        response = client.get_associated_role(
            GroupId=gg_group['Id']
        )
        if response['RoleArn'] == cfg['GgRoleArn']:
            logger.info(f"Role {cfg['GgRoleArn']} already registered")
        else:
            exists = False
    except Exception as e:
        logger.info(f"Role {cfg['GgRoleArn']} already registered")
        exists = False

    if exists == False:
        logger.info(f"Registering role {cfg['GgRoleArn']} to group {gg_group['Id']}")
        try:
            response = client.associate_role_to_group(
                GroupId=gg_group['Id'],
                RoleArn=cfg['GgRoleArn']
            )
        except Exception as e:
            logger.error(e)


def get_latest_group_version(cfg, gg_group):
    client = boto3.client('greengrass')
    try:
        response = client.get_group(
            GroupId=gg_group['Id'],
        )
        latest_version_arn = response['LatestVersionArn']
        latest_version_id = response['LatestVersion']
        logger.info(f"Latest group version: {latest_version_id} - {latest_version_arn}")
        return latest_version_arn, latest_version_id
    except Exception as e:
        logger.error(e)
        logger.info(f"Group {gg_group['Id']} has no version yet")
        return None, None


def get_group_definition(cfg, gg_group, version_arn):
    client = boto3.client('greengrass')
    try:
        response = client.get_group_version(
            GroupId=gg_group['Id'],
            GroupVersionId=version_arn
        )
        logger.info(f"Got group version info for group {gg_group['Id']}")
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
    logger.info(f"Creating logger definition")
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
                        'FunctionArn': cfg['GgFnArn'],
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


def create_group_version(gg_group, rd_arn, fn_arn, sub_arn, log_arn, core_arn):
    client = boto3.client('greengrass')
    logger.info(f"Creating new group version")
    try:
        response = client.create_group_version(
            CoreDefinitionVersionArn=core_arn,
            FunctionDefinitionVersionArn=fn_arn,
            GroupId=gg_group['Id'],
            ResourceDefinitionVersionArn=rd_arn,
            SubscriptionDefinitionVersionArn=sub_arn,
            LoggerDefinitionVersionArn=log_arn
        )
        return response
    except Exception as e:
        logger.error(e)
        return None


def delete_fs_resource(id):
    client = boto3.client('greengrass')
    try:
        logger.info(f"Deleting file system resource")
        response = client.delete_resource_definition(
            ResourceDefinitionId=id
        )
        return response
    except Exception as e:
        logger.error(e)
        return None


def delete_fn(id):
    client = boto3.client('greengrass')
    try:
        logger.info(f"Deleting lambda fn")
        response = client.delete_function_definition(
            FunctionDefinitionId=id
        )
        return response
    except Exception as e:
        logger.error(e)
        return None


def delete_sub_def(id):
    client = boto3.client('greengrass')
    try:
        logger.info(f"Deleting subscription {id}")
        response = client.delete_subscription_definition(
            SubscriptionDefinitionId=id
        )
    except Exception as e:
        logger.error(e)


def reset_deployments(gg_group):
    client = boto3.client('greengrass')
    logger.info(f"resetting deployments")

    try:
        response = client.reset_deployments(
            GroupId=gg_group['Id'],
            Force=True
        )
        return response
    except Exception as e:
        logger.error(e)
        return None


def get_cores(cfg):
    client = boto3.client('greengrass')
    logger.info(f"getting cores")
    try:
        response = client.list_core_definitions()
        return response['Definitions']
    except Exception as e:
        logger.error(e)
        return None


def get_core_defs(id):
    client = boto3.client('greengrass')
    logger.info(f"getting core def versions")
    try:
        response = client.list_core_definition_versions(
            CoreDefinitionId=id
        )
        return response['Versions']
    except Exception as e:
        logger.error(e)
        return None


def delete_group(gg_group):
    client = boto3.client('greengrass')
    logger.info(f"deleting group")
    try:
        response = client.delete_group(
            GroupId=gg_group['Id']
        )
        return response
    except Exception as e:
        logger.error(e)
        return None


def delete_thing(thing_name):
    # https://github.com/aws-samples/aws-iot-device-management-workshop/blob/master/bin/clean-up.py
    policy_names = {}
    iot_client = boto3.client('iot')
    try:
        r_principals = iot_client.list_thing_principals(thingName=thing_name)
    except Exception as e:
        logger.error("ERROR listing thing principals: {}".format(e))
        r_principals = {'principals': []}

    # logger.info("r_principals: {}".format(r_principals))
    for arn in r_principals['principals']:
        cert_id = arn.split('/')[1]
        logger.info("  arn: {} cert_id: {}".format(arn, cert_id))

        r_detach_thing = iot_client.detach_thing_principal(thingName=thing_name, principal=arn)
        logger.info("  DETACH THING: {}".format(r_detach_thing))

        r_upd_cert = iot_client.update_certificate(certificateId=cert_id, newStatus='INACTIVE')
        logger.info("  INACTIVE: {}".format(r_upd_cert))

        r_policies = iot_client.list_principal_policies(principal=arn)
        # logger.info("    r_policies: {}".format(r_policies))

        for pol in r_policies['policies']:
            pol_name = pol['policyName']
            logger.info("    pol_name: {}".format(pol_name))
            policy_names[pol_name] = 1
            r_detach_pol = iot_client.detach_policy(policyName=pol_name,target=arn)
            logger.info("    DETACH POL: {}".format(r_detach_pol))

        r_del_cert = iot_client.delete_certificate(certificateId=cert_id,forceDelete=True)
        logger.info("  DEL CERT: {}".format(r_del_cert))

    r_del_thing = iot_client.delete_thing(thingName=thing_name)
    logger.info("  DELETE THING: {}\n".format(r_del_thing))

    for p in policy_names:
        logger.info("DELETE policy: {}".format(p))
        try:
            r_del_pol = iot_client.delete_policy(policyName=p)
            logger.info("r_del_pol: {}".format(r_del_pol))
        except Exception as e:
            logger.error("ERROR: {}".format(e))


def delete_things(thing_arns):
    iot_client = boto3.client('iot')
    try:
        if len(thing_arns) > 0:
            list_things_response = iot_client.list_things()
            if 'things' in list_things_response:
                for thing in list_things_response['things']:
                    if thing['thingArn'] in thing_arns:
                        logger.info(f"deleting thingName {thing['thingName']}")
                        delete_thing(thing['thingName'])
    except Exception as e:
        logger.error(e)


def get_core_thing_arns(core_def):
    gg_client = boto3.client('greengrass')
    response = []
    try:
        core_def_version_response = gg_client.get_core_definition_version(
            CoreDefinitionId=core_def['Id'],
            CoreDefinitionVersionId=core_def['Version']
        )
        if 'Definition' in core_def_version_response:
            logger.info(core_def_version_response)
            response = [c['ThingArn'] for c in core_def_version_response['Definition']['Cores']]

        return response
    except Exception as e:
        logger.error(e)
        return response


def delete_core_thing(core_def):
    gg_client = boto3.client('greengrass')
    iot_client = boto3.client('iot')
    response = []
    try:
        core_def_version_response = gg_client.get_core_definition_version(
            CoreDefinitionId=core_def['Id'],
            CoreDefinitionVersionId=core_def['Version']
        )
        if 'Definition' in core_def_version_response:
            logger.info(core_def_version_response)
            core_thing_arns = [c['ThingArn'] for c in core_def_version_response['Definition']['Cores']]
            if len(core_thing_arns) > 0:
                logger.info(f"found core_thing_arns {core_thing_arns}")
                list_things_response = iot_client.list_things()
                if 'things' in list_things_response:
                    for thing in list_things_response['things']:
                        if thing['thingArn'] in core_thing_arns:
                            logger.info(f"deleting thingName {thing['thingName']}")
                            iot_client.delete_thing(
                                thingName=thing['thingName']
                            )
        return response
    except Exception as e:
        logger.error(e)
        return None


def delete_core_definition(core_def):
    client = boto3.client('greengrass')
    logger.info(f"deleting core definition")
    try:

        response = client.delete_core_definition(
            CoreDefinitionId=core_def['Id']
        )
        return response
    except Exception as e:
        logger.error(e)
        return None


def gg_cleanup(cfg, gg_groups):
    for group in gg_groups:
        reset_deployments(group)

    sub_defs = get_sub_definitions(cfg)
    for s in sub_defs:
        if cfg['DEF_UNIQUE_KEY'] in s['Name']:
            delete_sub_def(s['Id'])

    fn_defs = get_fn_definitions(cfg)
    for f in fn_defs:
        if cfg['DEF_UNIQUE_KEY'] in f['Name']:
            delete_fn(f['Id'])

    resource_defs = get_resource_definitions(cfg)
    for r in resource_defs:
        if cfg['DEF_UNIQUE_KEY'] in r['Name']:
            delete_fs_resource(r['Id'])

    for group in gg_groups:
        group_version_arn, group_version_id = get_latest_group_version(cfg, group)
        group_def = get_group_definition(cfg, group, group_version_id)
        if 'CoreDefinitionVersionArn' in group_def:
            for core in get_cores(cfg):
                for core_def in get_core_defs(core['Id']):
                    if group_def['CoreDefinitionVersionArn'] in core_def['Arn']:
                        thing_arns = get_core_thing_arns(core_def)
                        delete_core_definition(core_def)
                        delete_things(thing_arns)
        delete_group(group)


def gg_setup(cfg, gg_groups):
    for group in gg_groups:
        register_role(cfg, group)

    resource_defs = get_resource_definitions(cfg)
    exists = False
    for r in resource_defs:
        if cfg['DEF_UNIQUE_KEY'] in r['Name']:
            logger.info("FS resource already exists")
            fs_resource = r
            exists = True
            break
    if exists == False:
        fs_resource = create_fs_resource(cfg)

    fn_defs = get_fn_definitions(cfg)
    exists = False
    for f in fn_defs:
        if cfg['DEF_UNIQUE_KEY'] in f['Name']:
            logger.info("Fn already exists")
            fn_resource = f
            exists = True
            break
    if exists == False:
        fn_resource = create_fn(cfg)

    sub_defs = get_sub_definitions(cfg)
    subs_to_make = [('fit', cfg['GgFnArn'], 'fit/client/+/sent', 'cloud'),
        ('evaluate', cfg['GgFnArn'], 'evaluate/client/+/sent','cloud'),
        ('parameters', cfg['GgFnArn'], 'parameters/client/+/sent','cloud'),
        ('set', cfg['GgFnArn'], 'set/client/+/sent','cloud'),
        ('commands', 'cloud', 'commands/client/+/update', cfg['GgFnArn']),
        ('heartbeat', cfg['GgFnArn'], 'flower/clients/#','cloud')]
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

    for group in gg_groups:

        group_version_arn, group_version_id = get_latest_group_version(cfg, group)
        group_def = get_group_definition(cfg, group, group_version_id)

        gv = create_group_version(group, fs_resource['LatestVersionArn'], fn_resource['LatestVersionArn'], sub_resource['LatestVersionArn'], log_def_resource['LatestVersionArn'], group_def['CoreDefinitionVersionArn'])

        logger.info(f"Starting deployment for group {group['Id']}")
        client = boto3.client('greengrass')
        try:
            response = client.create_deployment(
                DeploymentType='NewDeployment',
                GroupId=group['Id'],
                GroupVersionId=gv['Version']
            )
        except Exception as e:
            logger.error(e)


def get_gg_groups(cfg):
    client = boto3.client('greengrass')
    try:
        response = client.list_groups()
        if 'Groups' in response:
            groups = [group for group in response['Groups'] if cfg['ProjectTag'] in group['Name']]
        logger.info(f"Found groups {[group['Name'] for group in groups]}")
        return groups
    except Exception as e:
        logger.error(e)
        return []


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

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--clean", action="store_true",
                        help="cleanup the greengrass configuration")
    args = parser.parse_args()

    cfg = read_cfg()
    logger.info(f"Found configuration data: {cfg}")

    gg_groups = get_gg_groups(cfg)

    if args.clean:
        logger.info("deleting gg configuration")
        gg_cleanup(cfg, gg_groups)
    else:
        logger.info("creating gg configuration")
        gg_setup(cfg, gg_groups)


