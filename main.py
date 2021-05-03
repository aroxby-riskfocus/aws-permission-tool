#!/usr/bin/env python3
import argparse
import sys


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--resources',
        help='ARNs to which permission should be granted',
        type=str,
        nargs='+',
        required=True,
    )
    parser.add_argument(
        '--grantees',
        help='ARNs which should be granted permission',
        type=str,
        nargs='+',
        required=True,
    )
    args = parser.parse_args(argv[1:])
    return args


def main(argv):
    args = parse_args(argv)
    print(args)


if __name__ == '__main__':
    main(sys.argv)
