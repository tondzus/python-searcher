import os
import sys
import time
import configparser
from operator import itemgetter
from itertools import chain, islice
from pkg_resources import resource_filename

from searcher.utils import iterate_words, document_count, files_iterator


def load_config(path=None):
    config = configparser.ConfigParser()
    config.read(resource_filename('searcher', 'conf.ini'))

    if os.path.isfile('searcher.ini'):
        config.read('searcher.ini')

    if path is not None:
        config.read(path)
    return config


def get_controller(config):
    backend_type = config.get('default', 'datastore')
    controller_func = {
        'sqlite3': sqlite_controller,
        'mongo': mongo_controller,
    }.get(backend_type, invalid_controller)
    return controller_func(config)


def sqlite_controller(config):
    from searcher.sqlite import SQLiteController
    return SQLiteController(config)


def mongo_controller(config):
    from searcher.mongo import MongoController
    return MongoController(config)


def invalid_controller(config):
    datastore = config.get('default', 'datastore')
    print('"{}" is not valid datastore type'.format(datastore), file=sys.stderr)
    sys.exit(1)


class Controller:
    def init(self, force=False):
        if force:
            self.document_store.clear()
            self.document_store.init()
            self.index_store.clear()
            self.index_store.init()
        else:
            self.document_store.init()
            self.index_store.init()

    def register(self, root):
        document_store = self.document_store
        documents_to_store, counter = [], 0
        msg = 'registering documents... {}/{}'
        document_total_count = document_count(root)
        for relative_path in files_iterator(root):
            with open(relative_path, errors='ignore') as fp:
                new_doc = document_store.prepare_document_query(fp.read())
                documents_to_store.append(new_doc)
                counter += 1
                print(msg.format(counter, document_total_count), end='\r')
            if len(documents_to_store) == self.document_batch_store_size:
                document_store.store_documents(documents_to_store)
                documents_to_store = []
        document_store.store_documents(documents_to_store)
        print('registered {} documents from {}'.format(counter, root))

    def index(self):
        msg = 'indexing documents... {}/{}'
        document_total_count = len(self.document_store)
        for count, document_id in enumerate(self.document_store, 1):
            document = self.document_store.load_document(document_id)
            document.indexer.index_document()
            self.index_store.register_document_indexes(document.indexer)
            print(msg.format(count, document_total_count), end='\r')
        print('indexed {} documents from datastore'.format(count))
        self.index_store.flush()

    def show(self, document_ids, preview=True):
        documents = map(self.document_store.load_document, document_ids)
        for doc in documents:
            if preview:
                print(doc.preview)
            else:
                print(doc.content)

    def query(self, query_string, measure=False, preview=False):
        query_limit = int(self.config.get('default', 'query_limit'))

        if measure:
            start = time.clock()

        query_parts = list(iterate_words(query_string))
        result_parts = [self.index_store.find_by_word(word, query_limit)
                        for word in query_parts]
        all_results_iterable = chain.from_iterable(result_parts)
        document_rank = dict()
        for doc_id, rank in all_results_iterable:
            if doc_id in document_rank:
                document_rank[doc_id] += rank
            else:
                document_rank[doc_id] = rank

        sorted_results = sorted(document_rank.items(), key=itemgetter(1),
                                reverse=True)
        results_with_ranks = islice(sorted_results, query_limit)
        results = list(map(lambda did: str(did[0]), results_with_ranks))

        if preview:
            self.show(results, preview=True)
        else:
            print(' '.join(results))

        if measure:
            print('Took {} to execute'.format(time.clock() - start))
