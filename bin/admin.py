#!/usr/bin/env python3
import argparse
import configparser
from searcher.controll import get_controller


def load_config(path):
    config = configparser.ConfigParser()
    config.read(path)
    return config


def main():
    config = load_config('searcher.ini')
    controller = get_controller(config)
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    subparsers.required = True
    subparsers.dest = 'action'

    document_parser = subparsers.add_parser('register', help='Import documents '
                                            'to document store')
    document_parser.add_argument('root', help='Path to document root folder')
    document_parser.set_defaults(func=controller.register)

    index_parser = subparsers.add_parser('index', help='Index documents in '
                                         'document store')
    index_parser.set_defaults(func=controller.index)

    query_parser = subparsers.add_parser('query', help='Execute search')
    query_parser.add_argument('query_string', help='Will use this to '
                              'search documents')
    query_parser.add_argument('-m', '--measure', action='store_true',
                              help='Print time taken to find results')
    query_parser.add_argument('-p', '--preview', action='store_true',
                              help='Print short preview for matches')
    query_parser.set_defaults(func=controller.query)

    init_parser = subparsers.add_parser('init', help='Prepare datastores')
    init_parser.add_argument('--force', action='store_true',
                             help='Delete datastores if they already exist')
    init_parser.set_defaults(func=controller.init)

    show_parser = subparsers.add_parser('show', help='Show documents')
    show_parser.add_argument('-p', '--preview', action='store_true',
                             help='Show shortened version of document')
    show_parser.add_argument('document_id', nargs='+')
    show_parser.set_defaults(func=controller.show)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
