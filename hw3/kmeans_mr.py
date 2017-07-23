#!/usr/bin/env python
import numpy as np
import subprocess
import matplotlib.pyplot as plt

import kmeans_utils
import mincemeat
import json
import time

PORT = 2001


def map_fn(k, point):
    import kmeans_utils
    centers, point = point
    new_k = tuple(centers[kmeans_utils.find_nearest_center(centers, point)])
    new_v = tuple(point)
    yield new_k, new_v


def reduce_fn(k, points):
    import kmeans_utils
    new_center = kmeans_utils.find_new_center(points)
    cost = kmeans_utils.compute_cost(new_center, points)
    return new_center, points, cost


def run_workers(n=1):
    # return [subprocess.Popen(['python', 'mincemeat.py', '-p', PASS, '127.0.0.1']) for i in range(n)]
    return subprocess.Popen(
        ['python', 'shepherd.py', '-s', '1', '-p', PASS, '-n', str(n), '-H', '127.0.0.1', '-P', str(PORT)])


def run_server(server):
    return server.run_server(password=PASS, port=PORT)


def save_centers(centers):
    with open("centers.json", "w+") as f:
        json.dump(centers, f)


def show_clustering(clustering):
    # Visualize results
    for center, points in clustering.items():
        points = np.array(points)
        plt.scatter(points[:, 0], points[:, 1], color=np.random.rand(3, 1), s=2)
    centers = np.array(list(clustering.keys()))
    plt.scatter(centers[:, 0], centers[:, 1], color='k', marker='P')
    plt.show()


if __name__ == '__main__':

    # Parameters
    PASS = 'changeme'
    STEP_DELTA_TH = 1
    K = 5
    NUM_POINTS = 500
    CONCURRENCY = 7

    # Init MR server
    s = mincemeat.Server()
    s.mapfn = map_fn
    s.reducefn = reduce_fn

    # Init data
    points = kmeans_utils.generate_random_points(num=NUM_POINTS, k=K)
    centers = kmeans_utils.initialize_centers(points, k=K)
    points = [(centers, point) for point in points]  # Join every point with the possible centers
    clustering = {}

    last_cost = None
    step_delta = STEP_DELTA_TH + 1
    i = 0
    start_time = time.time()
    while step_delta > STEP_DELTA_TH:
        s.datasource = dict(enumerate(points))

        run_workers(n=CONCURRENCY)
        results = run_server(s)
        clustering = {}
        cost = 0
        centers = []
        for old_center, new_clustering in results.items():
            new_center, new_points, partial_cost = new_clustering
            clustering.update({new_center: new_points})
            centers.append(new_center)
            cost += partial_cost

        if last_cost is not None:
            step_delta = last_cost - cost
        last_cost = cost

        points = [(centers, point) for (old_centers, point) in points]

        i += 1
        print("Finished iteration %s: Cost - %s, Elapsed Time: %s secs" % (i, cost, round(time.time() - start_time, 2)))

    print("Finished clustering!")
    show_clustering(clustering)

    exit(0)
