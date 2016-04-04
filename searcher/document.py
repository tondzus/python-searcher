import sqlite3
from searcher import utils
from searcher import index


class GenericDocument:
    """Representation of stored document. Can be queried for more information
    about this document and can be used to iterate over all words used in this
    document.
    """
    def __init__(self, document_id, uri):
        self.document_id = document_id
        self.uri = uri
        self.__indexer = None

    def __iter__(self):
        for line in self.content_iterator:
            yield from utils.iterate_words(line)

    @property
    def indexer(self):
        if self.__indexer is None:
            self.__indexer = index.DocumentIndexer(self)
        return self.__indexer

    @property
    def content(self):
        with open(self.uri) as fp:
            return fp.read()


class SQLiteDocumentStore:
    def __init__(self, dbpath):
        self.dbpath = dbpath
        self.conn = sqlite3.connect(dbpath)

    def load_document(self, document_id):
        cur = self.conn.cursor()
        query = 'SELECT path FROM documents WHERE id=?'
        result = cur.execute(query, (document_id, )).fetchone()
        return GenericDocument(document_id, result[0])

    def store_documents(self, paths):
        cur = self.conn.cursor()
        query = 'INSERT INTO documents (path) VALUES (?)'
        cur.executemany(query, paths)
        self.conn.commit()

    def __iter__(self):
        cur = self.conn.cursor()
        query = 'SELECT id FROM documents'
        yield from (result[0] for result in cur.execute(query))
