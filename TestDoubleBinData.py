import random

import numpy as np
from matplotlib import pyplot as plt
from scipy.stats import binned_statistic_2d


def make_double_bin_test_data(x1=[1, 3, 5], x2=[7, 9], y=[10, 12], stdev=0.2):
    data = []
    out_1 = []
    out_2 = []
    out_3 = []

    for i in x1:
        for j in x2:
            for k in y:
                data.append([i, j, k])

    for this_list in data:
        a = this_list[0]
        b = this_list[1]
        c = this_list[2]

        out_1.append(random.gauss(a, stdev))
        out_2.append(random.gauss(b, stdev))
        out_3.append(random.gauss(c, stdev))

    return out_1, out_2, out_3


def test_double_bin(x1, x2, y, binx, biny):
    return binned_statistic_2d(x1, x2, y, bins=[binx, biny], statistic='mean')


def ew_mean(a, window=2):
    return np.convolve(a, np.ones((window,)) / window, mode='valid')


def plot_double_bin(bin_results, x_label, y_label, legend_label):

    print(bin_results)

    plt_data = bin_results.statistic
    x_edge = ew_mean(bin_results.x_edge)
    y_edge = ew_mean(bin_results.y_edge)

    for i in range(plt_data.shape[0]):
        x = y_edge
        y = plt_data[i]
        plt.plot(x, y, label=x_edge[i])

    plt.legend(title=legend_label)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.grid()
    plt.show()



if __name__ == '__main__':

    x1, x2, y = make_double_bin_test_data()
    binx = [0, 2, 4, 6]
    biny = [6, 8, 10]
    plot_double_bin(test_double_bin(x1, x2, y, binx, biny), 'X-Name', 'Y-Name', 'L-Name')
