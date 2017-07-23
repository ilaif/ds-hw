#!/usr/bin/env python
import numpy as np
import subprocess
import matplotlib.pyplot as plt

import mincemeat
import time

PORT = 2000


def map_fn(k, v):
    words = v.split(" ")
    # print("map", (words[0], words[2]), words[1])
    yield (words[0], words[2]), words[1]


def reduce_fn(k, v):
    import itertools
    pairs = list(itertools.combinations(v, r=2))
    # print("reduce", pairs)
    return pairs


def run_workers(n=1):
    cmd = ['python', 'shepherd.py', '-s', '1', '-p', PASS, '-n', str(n), '-H', '127.0.0.1', '-P', str(PORT)]
    return subprocess.Popen(cmd)


def run_server(server):
    return server.run_server(password=PASS, port=PORT)


def show_clustering(clustering):
    # Visualize results
    for center, points in clustering.items():
        points = np.array(points)
        plt.scatter(points[:, 0], points[:, 1], color=np.random.rand(3, 1), s=2)
    centers = np.array(list(clustering.keys()))
    plt.scatter(centers[:, 0], centers[:, 1], color='k', marker='P')
    plt.show()


def load_text(file_path):
    with open(file_path, 'r') as f:
        return [x.strip() for x in f.readlines()]


if __name__ == '__main__':
    # Parameters
    PASS = 'changeme'
    CONCURRENCY = 2
    TEXT_FILE_PATH = 'pseudo_synonyms.txt'

    # Init MR server
    s = mincemeat.Server()
    s.mapfn = map_fn
    s.reducefn = reduce_fn

    # Init data
    data = load_text(TEXT_FILE_PATH)
    s.datasource = dict(enumerate(data))

    # Run
    start_time = time.time()
    run_workers(n=CONCURRENCY)
    results = run_server(s)

    pairs_count = {}
    for pairs in results.values():
        for pair in pairs:
            reverse_pair = (pair[1], pair[0])
            if pair not in pairs_count and reverse_pair not in pairs_count:
                pairs_count[pair] = 0
            pairs_count[pair] += 1

    answer = [pair for (pair, count) in pairs_count.items() if count > 1]

    print("Finished. Elapsed Time: %s secs.\nPairs: %s" % (round(time.time() - start_time, 2), answer,))
    exit(0)
