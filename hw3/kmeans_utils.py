import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as plt_colors

BASE_COLORS = list(plt_colors.BASE_COLORS)


def generate_random_points(num=100, k=3, var=3, mean_dist=5):
    rand_range = 2
    mean_x_noise = np.random.normal(0, rand_range, k)
    mean_y_noise = np.random.normal(0, rand_range, k)
    mean = [[mean_dist * i + mean_x_noise[i], mean_dist * i + mean_y_noise[i]] for i in range(k)]
    var_x_noise = np.random.normal(0, rand_range, k)
    var_y_noise = np.random.normal(0, rand_range, k)
    cov = [[[var + var_x_noise[i], 0], [0, var + var_y_noise[i]]] for i in range(k)]
    points = np.concatenate([np.random.multivariate_normal(mean[i], cov[i], num) for i in range(k)], axis=0)
    return [tuple(point) for point in points]


def initialize_centers(points, k=3):
    centroids = np.array(points)
    np.random.shuffle(centroids)
    return tuple([tuple(center) for center in centroids[:k]])


def compute_cost(center, points):
    center = np.array(center)
    points = np.array(points)
    return sum([(((center - point) ** 2).sum()) for point in points])


def find_nearest_center(centers, point):
    centers = np.array(centers)
    point = np.array(point)
    return (((centers - point) ** 2).sum(axis=1)).argmin()


def find_new_center(points):
    return tuple(np.array(points).mean(axis=0))


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


if __name__ == '__main__':
    data = generate_random_points()

    k = 3
    centers = initialize_centers(data, k=3)

    plt.interactive(False)

    mapping = np.array([find_nearest_center(centers, d) for d in data])

    colors = ['g', 'b', 'r', 'y', 'm', 'c', 'k'][:k]
    label_color_map = {i: colors[i] for i in range(k)}

    label_color = [label_color_map[c] for c in mapping]
    plt.scatter(data[:, 0], data[:, 1], color=label_color)
    plt.scatter(centers[:, 0], centers[:, 1], color='k', marker='P')
    plt.show()

    centers = np.array([find_new_center(data[mapping == i]) for i in range(k)])
    mapping = np.array([find_nearest_center(centers, d) for d in data])
    label_color = [label_color_map[c] for c in mapping]
    plt.scatter(data[:, 0], data[:, 1], color=label_color)
    plt.scatter(centers[:, 0], centers[:, 1], color='k', marker='P')
    plt.show()

    print(mapping)
    exit()
