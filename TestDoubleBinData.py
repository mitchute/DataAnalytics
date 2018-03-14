import random
import numpy as np
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


if __name__ == '__main__':
    x1, x2, y = make_double_bin_test_data()

    print(x1)
    print(x2)
    print(y)
    binx = [0, 2, 4, 6]
    biny = [6, 8, 10]
    ret = test_double_bin(x1, x2, y, binx, biny)

    print(ret)
    print(np.transpose(ret.statistic))
