# -*- coding:utf-8 -*-
import os
import sqlite3
from optparse import OptionParser
from datetime import datetime
import bz2
import gzip
import concurrent.futures
import json
import logging
import pickle
import threading

db_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'db')

logging.basicConfig(level=logging.DEBUG)

already_handle_set = set()
convert_path = './db/convert.bin'
if os.path.exists(convert_path):
    with open(convert_path, 'rb') as f:
        already_handle_set = pickle.load(f)
handle_set_lock = threading.Lock()


def handle_log(id: bytes, content: bytes, out_path: str):
    try:
        print(f'start handle {id}...')

        content = bz2.decompress(content)
        # 标准4麻类型
        if not b'type=\"169\"' in content:
            return

        mjlog_path = f'{out_path}/{id}.mjlog'
        mjson_path = f'{out_path}/{id}.mjson'

        content = gzip.compress(content)
        with open(mjlog_path, 'wb') as f:
            f.write(content)

        os.system(f'mjai convert {mjlog_path} {mjson_path}')
        with open(mjson_path, 'rb') as f:
            content = f.readlines()
            # handle scores
            new_content = []
            for i, line in enumerate(content):
                if line.startswith(b'{"type":"start_kyoku"'):
                    line_json = json.loads(line)
                    line_json['kyotaku'] = 0
                    last_line = json.loads(content[i-1])
                    if 'scores' in last_line.keys():
                        line_json['scores'] = last_line['scores']
                    else:
                        line_json['scores'] = [25000, 25000, 25000, 25000]
                    new_content.append(
                        bytes(json.dumps(line_json), encoding='utf-8')+b'\n')
                else:
                    new_content.append(line)

            content = b''.join(new_content)
            content = gzip.compress(content)
            with open(f'{mjson_path.replace(".mjson", ".json")}.gz', 'wb') as fw:
                fw.write(content)

        with handle_set_lock:
            print(
                f'handle {id} finished, out {out_path}, handled cnt {len(already_handle_set)}')

            already_handle_set.add(id)
            with open(convert_path, 'wb') as f:
                pickle.dump(already_handle_set, f)
    except Exception as e:
        logging.error("handle %s raised an exception: %s",
                      id, str(e), exc_info=True)


def main():
    global db_folder

    parser = OptionParser()
    parser.add_option('-y', '--year', type='string',
                      default=str(datetime.now().year), help='Target year')
    parser.add_option('-c', '--count', type='int', default=0)
    parser.add_option('-p', '--db_path', type='string')
    parser.add_option('-o', '--out_path', type='string')

    opts, _ = parser.parse_args()

    if opts.db_path:
        db_file = opts.db_path
    else:
        db_file = os.path.join(db_folder, f'{opts.year}.db')

    connection = sqlite3.connect(db_file)

    logs = []

    with connection:
        cursor = connection.cursor()

        cnt = opts.count
        if cnt == 0:
            cursor.execute('SELECT COUNT(*) FROM logs WHERE is_processed = 1;')
            cnt = cursor.fetchone()[0]

        print(f'start convert {cnt}')

        cursor.execute(f'SELECT * FROM logs LIMIT {cnt}')

        rows = cursor.fetchall()

        for row in rows:
            logs.append((row[0], row[6]))

    print(f'wait to process {len(logs)}...')
    cnt = len(logs)

    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
        tasks = []
        for i, (id, content) in enumerate(logs):
            with handle_set_lock:
                if id in already_handle_set:
                    continue

            tasks.append(executor.submit(handle_log, id, content, './logs'))

        # Wait for all worker tasks to finish
        concurrent.futures.wait(tasks)

    with open(convert_path, 'wb') as f:
        pickle.dump(already_handle_set, f)


if __name__ == '__main__':
    main()
