from searcher import utils
from searcher.indexer import DocumentIndexer


class GenericDocument:
    """Representation of stored document. Can be queried for more information
    about this document and can be used to iterate over all words used in this
    document.
    """
    def __init__(self, document_id, content):
        self.document_id = document_id
        self.content = content
        self.__indexer = None

    def __iter__(self):
        yield from utils.iterate_words(self.content)

    @property
    def indexer(self):
        if self.__indexer is None:
            self.__indexer = DocumentIndexer(self)
        return self.__indexer

    @property
    def preview(self):
        return self.content.split('\n', 1)[0]
