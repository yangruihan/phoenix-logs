# -*- coding:utf-8 -*-
import os
import sqlite3
from optparse import OptionParser
from datetime import datetime
import logging
import pickle

db_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'db')

logging.basicConfig(level=logging.DEBUG)

already_handle_set = set()

convert_set_path = os.path.join(db_folder, 'convert.bin')

if os.path.exists(convert_set_path) and os.path.getsize(convert_set_path) > 0:
    with open(convert_set_path, 'rb') as f:
        already_handle_set = pickle.load(f)


def fetch_data(conn, cnt, batch_size=1000):
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM logs LIMIT {cnt}')
    while True:
        data = cursor.fetchmany(batch_size)
        if not data:
            break
        yield data


def main():
    global db_folder

    parser = OptionParser()
    parser.add_option('-y', '--year', type='string',
                      default=str(datetime.now().year), help='Target year')
    parser.add_option('-p', '--db_path', type='string')

    opts, _ = parser.parse_args()

    db_file = None
    if opts.db_path:
        db_file = opts.db_path
    else:
        db_file = os.path.join(db_folder, f'{opts.year}.db')

    logging.info(f'start fix year {opts.year} db {db_file}')

    connection = sqlite3.connect(db_file)

    with connection:
        cursor = connection.cursor()

        cursor.execute('SELECT COUNT(*) FROM logs WHERE is_processed = 1;')
        cnt = cursor.fetchone()[0]

        logging.info(f'start fix {cnt}')
        handle_cnt = 0

        for rows in fetch_data(connection, cnt, batch_size=102400):
            for row in rows:

                id = row[0]

                mjson_path = f'./logs/{id}.mjson'
                gz_path = f'{mjson_path.replace(".mjson", ".json")}.gz'

                logging.info(f'start handle {id}, {handle_cnt + 1}/{cnt}')

                if os.path.exists(gz_path):
                    if os.path.getsize(gz_path) == 0:
                        logging.error(f'found error file {id}')
                        os.remove(gz_path)
                    else:
                        if id not in already_handle_set:
                            already_handle_set.add(id)

                handle_cnt += 1

    with open(convert_set_path, 'wb') as f:
        pickle.dump(already_handle_set, f)

    logging.info(
        f'handle finish, already handle cnt {len(already_handle_set)}')


if __name__ == '__main__':
    main()
