#!/usr/bin/env python3
import os
import sqlite3
import argparse
from io import StringIO


class EndOfFile(Exception): pass


def read_next_plot(file_pointer):
    start_text = {'MV: ', 'PL: ', 'BY: '}
    last_line, film = file_pointer.readline(), StringIO()
    while not last_line.startswith('-----------------'):
        if last_line[:4] in start_text:
            last_line = last_line[4:]
        film.write(last_line)
        film.write(' ')
        last_line = file_pointer.readline()
        if len(last_line) == 0:
            raise EndOfFile()
    return film.getvalue()[:-1]


def database(path):
    if os.path.isfile(path):
        return sqlite3.connect(path)
    conn = sqlite3.connect(path)
    conn.execute('CREATE TABLE documents('
                 'id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, '
                 'content TEXT);')
    return conn


def store(plots, db):
    query = 'INSERT INTO documents (content) VALUES (?)'
    c = db.cursor()
    c.executemany(query, plots)
    db.commit()


def parse_imdb_plots(path, db):
    with open(path, errors='ignore') as fp:
        counter, plots = 1, []
        while True:
            try:
                plot_text_list = read_next_plot(fp)
            except EndOfFile:
                print('Successfully imported {} movies'.format(counter))
                store(plots, db)
                break
            else:
                plots.append(tuple([plot_text_list]))
                counter += 1
                print('Imported {} movies so far'.format(counter), end='\r')

                if len(plots) == 1000:
                    store(plots, db)
                    plots = []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('plot_path', help='Path to imdb plot-no-header.list')
    parser.add_argument('db_path', default='documents.db', nargs='?',
                        help='Path to sqlite db to fill/create')
    args = parser.parse_args()

    db = database(args.db_path)
    try:
        parse_imdb_plots(args.plot_path, db)
    finally:
        db.close()


if __name__ == '__main__':
    main()
