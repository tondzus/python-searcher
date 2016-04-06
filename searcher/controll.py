import os
import sys
import time
from operator import itemgetter
from itertools import chain, islice


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
    def init(self, args):
        if args.force:
            self.document_store.clear()
            self.document_store.init()
            self.index_store.clear()
            self.index_store.init()
        else:
            self.document_store.init()
            self.index_store.init()

    def register(self, args):
        document_store = self.document_store
        documents_to_store, counter = [], 0
        for path, _, files in os.walk(args.root):
            for file_name in files:
                relative_path = os.path.join(path, file_name)
                with open(relative_path) as fp:
                    new_doc = document_store.prepare_document_query(fp.read())
                    documents_to_store.append(new_doc)
                    counter += 1
                    print('Registered {} documents'.format(counter), end='\r')

                if len(documents_to_store) == self.document_batch_store_size:
                    document_store.store_documents(documents_to_store)
                    documents_to_store = []
        document_store.store_documents(documents_to_store)
        print('Done registering {} documents from {}'.format(counter, args.root))

    def index(self, args):
        for count, document_id in enumerate(self.document_store, 1):
            document = self.document_store.load_document(document_id)
            document.indexer.index_document()
            self.index_store.register_document_indexes(document.indexer)
            msg = 'Indexed {} documents from datastore'
            print(msg.format(count), end='\r')
        print('Done indexing {} documents from datastore'.format(count))
        self.index_store.flush()

    def show(self, args):
        documents = map(self.document_store.load_document, args.document_id)
        for doc in documents:
            if args.preview:
                print(doc.preview)
            else:
                print(doc.content)

    def query(self, args):
        query_limit = int(self.config.get('default', 'query_limit'))

        if args.measure:
            start = time.clock()

        query_parts = args.query_string.split()
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

        if args.preview:
            args.document_id = results
            self.show(args)
        else:
            print(' '.join(results))

        if args.measure:
            print('Took {} to execute'.format(time.clock() - start))
