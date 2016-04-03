import csv
import sqlite3
from collections import Counter


class DocumentIndexer:
    def __init__(self, document):
        self.total_word_count = 0
        self.document = document
        self.index = Counter()

    def index_document(self):
        for word in self.document:
            self.index[word] += 1
            self.total_word_count += 1

    def __iter__(self):
        did = self.document.document_id
        yield from ((did, w, float(c) / self.total_word_count)
                    for w, c in self.index.items())


class SQLiteIndexStore:
    def __init__(self, dbpath):
        self.dbpath = dbpath
        self.conn = sqlite3.connect(dbpath)
        self.unsaved_indexes = []
        self.max_query_length = 10000
        self.next_index_id = 1
        self.query = 'INSERT INTO indexes (document_id, word, rank) ' \
                     'VALUES (?, ?, ?)'

    def register_document_indexes(self, index_document):
        self.unsaved_indexes.extend(index_document)
        if len(self.unsaved_indexes) > self.max_query_length:
            self.store_indexes(self.unsaved_indexes[:self.max_query_length])
            self.unsaved_indexes = self.unsaved_indexes[self.max_query_length:]

    def create_new_indexes(self, table_id, index_list):
        q = self.query.format(table_id)
        cur = self.conn.cursor()
        cur.executemany(q, index_list)
        self.conn.commit()

    def store_indexes(self, indexes=None):
        if indexes is None:
            self.unsaved_indexes, indexes = [], self.unsaved_indexes

        indexes = [(iid, did, w, r)
                   for iid, (did, w, r) in enumerate(indexes, self.next_index_id)]
        self.next_index_id = indexes[-1][0] + 1

        with open('indexes.csv', 'a') as fp:
            writer = csv.writer(fp)
            writer.writerows(indexes)

    def find_by_word(self, word, limit=None):
        if limit:
            q = 'SELECT document_id, rank ' \
                'FROM indexes ' \
                'WHERE word=? ' \
                'ORDER BY rank DESC ' \
                'LIMIT {} '.format(limit)
        else:
            q = 'SELECT document_id, rank FROM indexes WHERE word=? ' \
                'ORDER BY rank DESC'
        cur = self.conn.cursor()
        cur.execute(q, (word, ))
        yield from cur.fetchall()
