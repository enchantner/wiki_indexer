#!/usr/bin/env python3

import json
import queue
import argparse

from db import DBClient
from crawler import WikiCrawler
from indexer import WordIndexer


def run(threads=5):
    urls = queue.Queue()

    with open("start.txt", "r") as startlist:
        for url in startlist:
            urls.put(url)

    pool = [WikiCrawler(urls) for _ in range(threads)]
    list(map(
        lambda t: t.start(),
        pool
    ))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        dest="action", help='actions'
    )
    run_parser = subparsers.add_parser(
        'run', help='run application locally'
    )
    run_parser.add_argument(
        '-t', '--threads', dest='threads', action='store', type=int,
        help='number of threads to start', default=5
    )
    query_parser = subparsers.add_parser(
        'query', help='make a query to indexer'
    )
    query_parser.add_argument(
        '-l', '--limit', dest='limit', action='store', type=int,
        help='how many words to print'
    )
    drop_parser = subparsers.add_parser(
        'drop', help='drop indexer database'
    )

    params, other_params = parser.parse_known_args()
    if params.action == "query":
        indexer = WordIndexer()
        result = indexer.query(limit=params.limit)
        print(json.dumps(result, indent=4))
    elif params.action == "run":
        run(threads=params.threads)
    elif params.action == "drop":
        client = DBClient()
        client.drop_database(client.db_name)
    else:
        parser.print_help()
