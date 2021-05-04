#!/usr/bin/env python3
import argparse
import sys


SERVICE_MAP = {
    'quicksight': {
        'list': {
            'user': 'list_users',
            'dataset': 'list_data_sets',
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
            if isinstance(resource, str):
                self.resource_type, self.resource_id = resource.split('/', 1)
            elif isinstance(resource, list):
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


class FindResource(object):
    def __init__(self, kv_dict):
        self.kwargs = kv_dict.copy()
        try:
            self.service = self.kwargs.pop('FindService')
            self.resource_type = self.kwargs.pop('FindResourceType')
        except KeyError:
            raise KeyError(
                'Both "FindService" and "FindResourceType" must be specified for --find\'s'
            )
        if not self.kwargs:
            raise ValueError('All --find\'s must specify at least one searchable attribute')

    def __str__(self):
        return f'{self.service}:{self.resource_type}/{self.kwargs}'


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


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--find',
        help=(
            'Find RESOURCE or GRANTEE by attribute. '
            'Note: The attribute FindService and FindResourceType is required. '
            'Eg: --find FindService=quicksight FindResourceType=user Email=andy.roxby@riskfocus.com'
        ),
        type=str,
        metavar='KEY=VALUE',
        action='append',
        nargs='+',
    )
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

    args = parser.parse_args(argv[1:])
    return args


def main(argv):
    args = parse_args(argv)
    # Continue to parse and validate args
    resources = [Arn(resource) for resource in args.resources or ()]
    grantees = [Arn(grantee) for grantee in args.grantees or ()]
    find = [FindResource(d) for d in _key_value_pairs_to_dicts(args.find or ())]
    print([str(_) for _ in resources])
    print([str(_) for _ in grantees])
    print([str(_) for _ in find])


if __name__ == '__main__':
    main(sys.argv)
