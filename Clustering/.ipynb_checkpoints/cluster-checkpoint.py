import math


class CLuster:
    def __init__(self, idx, n_attr):
        self.cluster_idx = idx
        self.stat = STAT(n_attr)

    def get_stat(self):
        return self.stat

    def set_stat(self, stat):
        self.stat = stat

    def update_stat_w_point(self, point):
        self.stat.update_w_point(point)

    def update_stat_w_cluster(self, cluster):
        self.stat.update_w_cluster(cluster)


class STAT:
    def __init__(self, n_attr):
        self.n_attr = n_attr
        self.n_points = 0
        self.centroid = [0] * n_attr
        self.sum = [0] * n_attr
        self.sumsq = [0] * n_attr

    def get_sum(self):
        return self.sum

    def get_sumsq(self):
        return self.sumsq

    def get_centroid(self):
        return self.centroid

    def get_n_points(self):
        return self.n_points

    def update_w_point(self, point):
        self.n_points += 1
        for i in range(self.n_attr):
            self.sum[i] += point[i]
            self.sumsq[i] += math.pow(point[i], 2)
            self.centroid[i] = self.sum[i] / self.n_points

    def update_w_cluster(self, cluster):
        cluster_stat = cluster.get_stat()
        cluster_sum = cluster_stat.get_sum()
        cluster_sumsq = cluster_stat.get_sumsq()
        self.n_points += cluster_stat.get_n_points()

        for i in range(self.n_attr):
            self.sum[i] += cluster_sum[i]
            self.sumsq[i] += cluster_sumsq[i]
            self.centroid[i] = self.sum[i] / self.n_points
