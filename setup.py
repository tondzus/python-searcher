from setuptools import setup


setup(
    name='pythonsearcher',
    version='0.1',
    description='Simple fulltext search engine',
    author='Tomas Stibrany',
    author_email='tms.stibrany@gmail.com',
    url='https://github.com/tondzus/python-searcher',
    scripts=['bin/imdb.py', 'bin/admin.py', 'bin/spark-indexer.py'],
    packages=['searcher'],
    package_data={'': ['conf.ini']},
    install_requires=['plumbum'],
)
