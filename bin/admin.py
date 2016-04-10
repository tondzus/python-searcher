#!/usr/bin/env python3
from plumbum import cli, local
from searcher.controll import get_controller, load_config


@cli.Predicate
def ExistingPath(val):
    p = local.path(val)
    if not p.exists():
        raise ValueError('{} does not exist'.format(val))
    return p


class PythonSearcher(cli.Application):
    """Python fulltext search engine CLI"""
    VERSION = '0.1'

    def main(self):
        config = load_config('searcher.ini')
        self.controller = get_controller(config)


@PythonSearcher.subcommand('init')
class PythonSearcherInit(cli.Application):
    """Initialize datastores"""
    force = cli.Flag(['-f', '--force'], help='Remove old database if present')

    def main(self):
        self.root_app.controller.init(self.force)


@PythonSearcher.subcommand('register')
class PythonSearcherRegister(cli.Application):
    """Import documents into database"""
    def main(self, root: ExistingPath):
        self.root_app.controller.register(root)


@PythonSearcher.subcommand('index')
class PythonSearcherIndex(cli.Application):
    """Index documents in datastore. Takes long time. You've been warned"""
    use_spark = cli.Flag('--spark', help='Utilize spark to index documents')

    def main(self):
        self.root_app.controller.index(self.use_spark)


@PythonSearcher.subcommand('search')
class PythonSearcherSearch(cli.Application):
    """Search database for index hits for query string"""
    preview = cli.Flag(['-p', '--preview'],
                       help='Show preview for found matches')
    measure = cli.Flag(['-m', '--measure'],
                       help='Print how long it took to deliver results')

    def main(self, query_string):
        opts = {'preview': self.preview, 'measure': self.measure}
        self.root_app.controller.query(query_string, **opts)


@PythonSearcher.subcommand('show')
class PythonSearcherShow(cli.Application):
    """Show document from database"""
    preview = cli.Flag(['-p', '--preview'],
                       help='Show only short preview')

    def main(self, document_id, *document_ids):
        document_ids = [document_id] + list(document_ids)
        self.root_app.controller.show(document_ids, self.preview)


def main():
    PythonSearcher.run()


if __name__ == '__main__':
    main()
