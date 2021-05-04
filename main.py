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
        },
        'describe': {
            'dataset': 'describe_data_set',
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

        try:
            self.listing_fn = SERVICE_MAP[self.service]['list'][self.type]['fn']
            self.listing_key = SERVICE_MAP[self.service]['list'][self.type]['key']
            self.listing_params = SERVICE_MAP[self.service]['list'][self.type]['params']
            self.is_grantee = self.type in SERVICE_MAP[self.service]['grantees']
        except KeyError:
            raise KeyError(f'{self.service}:{self.type} is not supported for search')
        if not self.is_grantee and self.type not in SERVICE_MAP[self.service]['describe']:
            raise KeyError(f'{self.service}:{self.type} is not supported as a resource')

    def find_arn(self, aws_account_id):
        api_params = {key: value for key, value in {
            'AwsAccountId': aws_account_id,
            'Namespace': 'default',
        }.items() if key in self.listing_params}

        client = boto3.client(self.service)
        data = getattr(client, self.listing_fn)(**api_params)[self.listing_key]

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


def get_aws_account_id():
    return boto3.client('sts').get_caller_identity().get('Account')


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--grantees',
        help='ARNs which should be granted permission',
        type=str,
        metavar='GRANTEE',
        nargs='*',
    )
    parser.add_argument(
        '--resources',
        help='ARNs to which permission should be granted',
        type=str,
        metavar='RESOURCE',
        nargs='*',
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

    print([str(_) for _ in grantees])
    print([str(_) for _ in resources])


if __name__ == '__main__':
    main(sys.argv)
