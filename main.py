#!/usr/bin/env python3
import argparse
import sys

import boto3


SERVICE_MAP = {
    'quicksight': {
        'list': {
            'user': {
                'fn': 'list_users',
                'key': 'UserList',
                'params': ('AwsAccountId', 'Namespace', )
            },
            'dataset': {
                'fn': 'list_data_sets',
                'key': 'DataSetSummaries',
                'params': ('AwsAccountId', )
            },
            'datasource': {
                'fn': 'list_data_sources',
                'key': 'DataSources',
                'params': ('AwsAccountId', )
            },
        },
        'desc-perms': {
            'dataset': {
                'fn': 'describe_data_set_permissions',
                'key': 'Permissions',
                'params': ('AwsAccountId', 'DataSetId')
            },
            'datasource': {
                'fn': 'describe_data_source_permissions',
                'key': 'Permissions',
                'params': ('AwsAccountId', 'DataSourceId')
            },
        },
        'grant-perms': {
            'dataset': {
                'fn': 'update_data_set_permissions',
                'key': 'Status',
                'params': ('AwsAccountId', 'DataSetId', 'GrantPermissions'),
            },
            'datasource': {
                'fn': 'update_data_source_permissions',
                'key': 'Status',
                'params': ('AwsAccountId', 'DataSourceId', 'GrantPermissions'),
            },
        },
        'grantees': ('user'),
    },
}


class Arn(object):
    def __init__(self, arn):
        try:
            self.const, self.partition, self.service, self.region, \
                self.account_id, *resource = arn.split(':')
        except ValueError:
            raise ValueError(f'Invalid ARN: {arn}')

        if self.const != 'arn' or self.partition != 'aws':
            raise ValueError(f'{arn} is not an AWS ARN')

        try:
            if len(resource) == 1:
                resource = resource[0]
                self.resource_type, self.resource_id = resource.split('/', 1)
            elif len(resource) == 2:
                self.resource_type, self.resource_id = resource
            else:
                raise ValueError
        except ValueError:
            raise ValueError(f'Invalid resource-type and resource-id specification "{resource}"')

    def __str__(self):
        return (
            f'{self.const}:{self.partition}:{self.service}:{self.region}:'
            f'{self.account_id}:{self.resource_type}/{self.resource_id}'
        )


class ResourceSearch(object):
    def __init__(self, kv_dict):
        self.kwargs = kv_dict.copy()
        try:
            self.service = self.kwargs.pop('service')
            self.type = self.kwargs.pop('type')
        except KeyError:
            raise KeyError('Both "service" and "type" must be specified for --search\'s')
        if not self.kwargs:
            raise ValueError('All --search\'s must specify at least one searchable attribute')

        is_listable = self.type in SERVICE_MAP[self.service]['list']
        is_describable = self.type in SERVICE_MAP[self.service]['desc-perms']
        is_updatable = self.type in SERVICE_MAP[self.service]['grant-perms']
        self.is_grantee = self.type in SERVICE_MAP[self.service]['grantees']
        if not is_listable:
            raise KeyError(f'{self.service}:{self.type} is not supported for search')
        if not self.is_grantee and (not is_describable or not is_updatable):
            raise KeyError(f'{self.service}:{self.type} is not supported as a resource')

    def find_arn(self, aws_account_id):
        data = _make_api_call(
            self.service, self.type, 'list', {
                'AwsAccountId': aws_account_id,
                'Namespace': 'default',
            },
        )

        matches = []
        for entity in data:
            if all(entity.get(key) == value for key, value in self.kwargs.items()):
                matches.append(entity)
        if not matches:
            raise ValueError(f'No matches for {self.kwargs}')
        if len(matches) > 1:
            for match in matches:
                print(match)
            raise ValueError(f'Multiple matches for {self.kwargs}')

        return matches[0]['Arn']

    def __str__(self):
        return f'{self.service}:{self.type}/{self.kwargs}'


def _flatten(lst):
    output = []
    for x in lst:
        for y in x:
            output.append(y)
    return output


def _key_value_pairs_to_dicts(kvps):
    dicts = []
    for kvp in kvps:
        parts = _flatten(kv.split('=') for kv in kvp)
        dicts.append(dict(zip(parts[::2], parts[1::2])))
    return dicts


def _make_api_call(service, resource_type, verb, api_params):
    use_fn = SERVICE_MAP[service][verb][resource_type]['fn']
    use_key = SERVICE_MAP[service][verb][resource_type]['key']
    use_params = SERVICE_MAP[service][verb][resource_type]['params']

    api_params = {key: value for key, value in api_params.items() if key in use_params}

    client = boto3.client(service)
    data = getattr(client, use_fn)(**api_params)

    try:
        data = data[use_key]
    except KeyError:
        print(data.keys())
        raise
    return data


def get_aws_account_id():
    return boto3.client('sts').get_caller_identity().get('Account')


def get_best_permissions(arn):
    data = _make_api_call(
        arn.service, arn.resource_type, 'desc-perms', {
            'AwsAccountId': arn.account_id,
            'Namespace': 'default',
            'DataSetId': arn.resource_id,
            'DataSourceId': arn.resource_id,
        },
    )

    # HACK this returns the most specific permissions NOT the "best"
    best_permissions = []
    for permission_block in data:
        actions = permission_block['Actions']
        if len(actions) > len(best_permissions):
            best_permissions = actions[:]

    return best_permissions


def grant_permissions(resource, actions, grantee):
    data = _make_api_call(
        resource.service, resource.resource_type, 'grant-perms', {
            'AwsAccountId': resource.account_id,
            'Namespace': 'default',
            'DataSetId': resource.resource_id,
            'DataSourceId': resource.resource_id,
            'GrantPermissions': ({
                'Principal': str(grantee),
                'Actions': actions,
            },)
        },
    )

    return data


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--grantees',
        help='ARNs which should be granted permission',
        type=str,
        metavar='GRANTEE',
        action='extend',
        nargs='+',
    )
    parser.add_argument(
        '--resources',
        help='ARNs to which permission should be granted',
        type=str,
        metavar='RESOURCE',
        action='extend',
        nargs='+',
    )
    parser.add_argument(
        '--search',
        help=(
            'Find RESOURCE or GRANTEE by attribute. '
            'Note: The attribute service and type is required. '
            'Eg: --search service=quicksight type=user Email=andy.roxby@riskfocus.com'
        ),
        type=str,
        metavar='KEY=VALUE',
        action='append',
        nargs='+',
    )

    args = parser.parse_args(argv[1:])
    return args


def main(argv):
    args = parse_args(argv)
    # Continue to parse and validate args
    grantees = [Arn(grantee) for grantee in args.grantees or ()]
    resources = [Arn(resource) for resource in args.resources or ()]
    searches = [ResourceSearch(d) for d in _key_value_pairs_to_dicts(args.search or ())]

    if not any((grantees, resources, searches)):
        raise RuntimeError('Nothing to do')

    aws_account_id = get_aws_account_id()

    for search in searches:
        arn = search.find_arn(aws_account_id)
        if search.is_grantee:
            grantees.append(Arn(arn))
        else:
            resources.append(Arn(arn))

    for resource in resources:
        print(f'Processing {str(resource)}')
        permissions = get_best_permissions(resource)
        for grantee in grantees:
            status = grant_permissions(resource, permissions, grantee)
            if status != 200:
                msg = f'Unexpected status {status} while updating {str(resource)} for {str(grantee)}'
                print(msg, file=sys.stderr)

    print('Done!')


if __name__ == '__main__':
    main(sys.argv)
