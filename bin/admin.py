#!/usr/bin/env python3
import os
import time
import sqlite3
import argparse
from operator import itemgetter
from itertools import chain
from itertools import islice

from searcher.document import SQLiteDocumentStore
from searcher.index import SQLiteIndexStore


def init(args):
    if args.datastore in ['index', 'all']:
        init_index_store(args.index_store, args.force)
    if args.datastore in ['document', 'all']:
        init_document_store(args.document_store, args.force)


def init_document_store(document_store_connector, force):
    if os.path.isfile(document_store_connector):
        if force:
            os.remove(document_store_connector)
        else:
            template = 'WARNING: Document store {} already exist, won\'t overwrite'
            print(template.format(document_store_connector))
            return

    conn = sqlite3.connect(document_store_connector)
    conn.execute('CREATE TABLE documents('
                 'id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, '
                 'content TEXT);')
    conn.close()


def init_index_store(index_store_connector, force):
    if os.path.isfile(index_store_connector):
        if force:
            os.remove(index_store_connector)
        else:
            template = 'WARNING: Index store {} already exist, won\'t overwrite'
            print(template.format(index_store_connector))
            return

    conn = sqlite3.connect(index_store_connector)
    conn.execute('CREATE TABLE indexes('
                 'id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, '
                 'document_id INTEGER NOT NULL, '
                 'word CHAR(50) NOT NULL, '
                 'rank FLOAT NOT NULL);')
    conn.execute('CREATE INDEX indexes_document_id_idx '
                 'ON indexes (document_id)')
    conn.execute('CREATE INDEX indexes_word_idx ON indexes (word)')
    conn.close()


def index_documents(args):
    document_store_connector = args.document_store
    index_store_connector = args.index_store
    document_store = SQLiteDocumentStore(document_store_connector)
    index_store = SQLiteIndexStore(index_store_connector)
    for count, document_id in enumerate(document_store, 1):
        document = document_store.load_document(document_id)
        document.indexer.index_document()
        index_store.register_document_indexes(document.indexer)
        msg = 'Indexed {} documents from {}'.format(count,
                                                    document_store_connector)
        print(msg, end='\r')
    template = 'Successfully indexed all documents from {}'
    print(template.format(document_store_connector))


def query(args):
    index_store_connector, query_string = args.index_store, args.query_string
    index_store = SQLiteIndexStore(index_store_connector)
    query_limit = 10
    
    if args.measure:
        start = time.clock()

    query_parts = query_string.split()
    result_parts = [index_store.find_by_word(word, query_limit)
                    for word in query_parts]
    all_results_iterable = chain.from_iterable(result_parts)
    sorted_results = sorted(all_results_iterable, key=itemgetter(1),
                            reverse=True)
    results_with_ranks = islice(sorted_results, query_limit)
    results = list(map(lambda did: str(did[0]), results_with_ranks))

    if args.preview:
        args.document_id = results 
        show(args)
    else:
        print(' '.join(results))

    if args.measure:
        print('Took {} to execute'.format(time.clock() - start))


def show(args):
    document_store = SQLiteDocumentStore(args.document_store)
    documents = map(document_store.load_document, args.document_id)
    for doc in documents:
        if args.preview:
            first_line = doc.content.split('\n', 1)[0]
            print(first_line)
        else:
            print(doc.content)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--index_store', default='indexes.db',
                        help='Connector to index datastore')
    parser.add_argument('--document_store', default='documents.db',
                        help='Connector to document datastore')

    subparsers = parser.add_subparsers()
    subparsers.required = True
    subparsers.dest = 'action'

    index_parser = subparsers.add_parser('index', help='Index documents in '
                                         'document store')
    index_parser.set_defaults(func=index_documents)

    query_parser = subparsers.add_parser('query', help='Execute search')
    query_parser.add_argument('query_string', help='Will use this to '
                              'search documents')
    query_parser.add_argument('-m', '--measure', action='store_true',
                              help='Print time taken to find results')
    query_parser.add_argument('-p', '--preview', action='store_true',
                              help='Print short preview for matches')
    query_parser.set_defaults(func=query)

    init_parser = subparsers.add_parser('init', help='Prepare datastores')
    init_parser.add_argument('datastore', choices=['document', 'index', 'all'],
                             default='all', nargs='?')
    init_parser.add_argument('--force', action='store_true',
                             help='Delete datastores if they already exist')
    init_parser.set_defaults(func=init)

    show_parser = subparsers.add_parser('show', help='Show documents')
    show_parser.add_argument('-p', '--preview', action='store_true',
                             help='Show shortened version of document')
    show_parser.add_argument('document_id', nargs='+')
    show_parser.set_defaults(func=show)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
