#!/usr/bin/env python
import numpy as np
import subprocess
import matplotlib.pyplot as plt

import mincemeat
import time


def map_fn(k, v):
    d = v.split("->")
    vertex = d[0].strip()
    neighbours = set(d[1].strip().split(" "))
    print("map", vertex, neighbours)
    yield vertex, len(neighbours)


def reduce_fn(k, v):
    return v


def run_workers(n=1):
    return subprocess.Popen(['python', 'shepherd.py', '-s', '1', '-p', PASS, '-n', str(n), '-H', '127.0.0.1'])


def run_server(server):
    return server.run_server(password=PASS)


def show_clustering(clustering):
    # Visualize results
    for center, points in clustering.items():
        points = np.array(points)
        plt.scatter(points[:, 0], points[:, 1], color=np.random.rand(3, 1), s=2)
    centers = np.array(list(clustering.keys()))
    plt.scatter(centers[:, 0], centers[:, 1], color='k', marker='P')
    plt.show()


def load_graph(file_path):
    with open(file_path, 'r') as f:
        return [x.strip() for x in f.readlines()]


if __name__ == '__main__':
    # Parameters
    PASS = 'changeme'
    CONCURRENCY = 2
    GRAPH_FILE_PATH = 'graph.txt'

    # Init MR server
    s = mincemeat.Server()
    s.mapfn = map_fn
    s.reducefn = reduce_fn

    # Init data
    data = load_graph(GRAPH_FILE_PATH)
    s.datasource = dict(enumerate(data))

    # Run
    start_time = time.time()
    run_workers(n=CONCURRENCY)
    results = run_server(s)
    num_vertices = len(results.keys())
    is_full_clique = True
    for vertex, num_neighbours in results.items():
        num_neighbours = num_neighbours[0]
        if num_neighbours < num_vertices - 1:
            is_full_clique = False

    print("Finished. Is Full Clique: %s, Elapsed Time: %s secs" % (is_full_clique, round(time.time() - start_time, 2)))
    exit(0)
