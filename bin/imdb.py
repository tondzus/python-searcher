#!/usr/bin/env python3
import os
import sys
import uuid
import argparse
from io import StringIO


class EndOfFile(Exception): pass


def read_next_plot(file_pointer):
    start_text = {'MV: ', 'PL: ', 'BY: '}
    last_line, film = file_pointer.readline(), StringIO()
    if len(last_line) == 0:
        raise EndOfFile()
    while not (last_line.startswith('-----------------') or len(last_line) == 0):
        if last_line[:4] in start_text:
            last_line = last_line[4:]
        film.write(last_line)
        film.write(' ')
        last_line = file_pointer.readline()
    return film.getvalue()[:-1]


def prepare_folder(out_dir):
    if os.path.isfile(out_dir):
        print('ERROR: {} is file'.format(out_dir), file=sys.stderr)
        sys.exit(1)
    if not os.path.isdir(out_dir):
        try:
            os.mkdir(out_dir)
        except FileNotFoundError:
            msg = 'ERROR: Can\'t create {}. Please create yourself and rerun'
            print(msg.format(out_dir), file=sys.stderr)
            sys.exit(1)


def store_imdb_plot(out_dir, text):
    name = str(uuid.uuid4())
    with open(os.path.join(out_dir, name), 'w') as fp:
        fp.write(text)


def parse_imdb_plots(plot_path, out_dir):
    with open(plot_path, errors='ignore') as fp:
        counter = 0
        while True:
            try:
                plot_text = read_next_plot(fp)
            except EndOfFile:
                print('Successfully imported {} movies'.format(counter))
                break
            else:
                store_imdb_plot(out_dir, plot_text)
                counter += 1
                print('Imported {} movies so far'.format(counter), end='\r')


def main():
    parser = argparse.ArgumentParser(description='Parse plot.list into '
                                     'indexable plot files')
    parser.add_argument('plot_path', help='Path to imdb plot-no-header.list')
    parser.add_argument('out_dir', help='Film files are going to be stored '
                        'in this directory')
    args = parser.parse_args()

    prepare_folder(args.out_dir)
    parse_imdb_plots(args.plot_path, args.out_dir)


if __name__ == '__main__':
    main()
