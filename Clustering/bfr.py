from utils import *
from cluster import CLuster
import copy

MAX_ITERATION = 1000
MAX_DISTANCE = 10000.0


class BFR:

    def __init__(self, n_attr, n_cluster, dt_idx_dict, cluster_result):
        self.n_cluster = n_cluster
        self.n_attr = n_attr  # number of features
        self.discard_set = []  # containing clusters
        self.compress_set = []  # containing clusters
        self.retained_set = []  # containing points
        self.cur_cluster_idx = 0
        self.dt_idx_dict = dt_idx_dict
        self.cluster_result = cluster_result

    def init(self, dt_idx):
        self.retained_set = copy.copy(dt_idx)
        self.init_ds(dt_idx)  # initialization discard set
        self.init_cs()  # initialization compress set

    def init_ds(self, dt_idx):
        data = [self.dt_idx_dict[i] for i in dt_idx]
        labels, _ = get_k_means_label(data, self.n_cluster * 10, max_iter=100)

        remain_points = []
        for n_cluster in range(self.n_cluster * 10):
            point_idx_list = [dt_idx[i] for i, label in enumerate(labels) if label == n_cluster]
            if len(point_idx_list) > 1: 
                remain_points += point_idx_list

        data = [self.dt_idx_dict[i] for i in remain_points]
        new_labels, _ = get_k_means_label(data, self.n_cluster, max_iter=100)

        for n_cluster in range(self.n_cluster):
            label_idx = [remain_points[i] for i, label in enumerate(new_labels) if label == n_cluster]
            if len(label_idx) > 1:
                new_cluster = CLuster(self.cur_cluster_idx, self.n_attr)  # build a new cluster
                self.cur_cluster_idx += 1  # update current number of clusters in the BFR
                for point_idx in label_idx:
                    new_cluster.update_stat_w_point(self.dt_idx_dict[point_idx])  # update stat of the new cluster
                    self.cluster_result[point_idx] = new_cluster.cluster_idx
                    self.retained_set.remove(point_idx)  # remove this point from the retained set
                self.discard_set.append(new_cluster)

        # if len(self.retained_set):
        #     self.assign_rs_to_ds()

    def init_cs(self):
        cs, rs = self.generate_clusters(self.retained_set, self.n_cluster * 10)
        self.retained_set = rs
        self.compress_set += cs

    def bfr_main(self, dt_idx):
        self.retained_set += dt_idx

        if len(self.retained_set):
            self.assign_rs_to_ds()

        if len(self.retained_set):
            self.assign_rs_to_cs()

        if len(self.retained_set):
            cs, rs = self.generate_clusters(self.retained_set, self.n_cluster * 10)
            self.retained_set = rs
            self.compress_set += cs

        if len(self.compress_set):
            self.merge_cs()

    def assign_rs_to_ds(self):
        rs = copy.copy(self.retained_set)
        for p_idx in rs:
            point = self.dt_idx_dict[p_idx]
            min_cluster = find_nearest_cluster_for_point(point, self.discard_set)
            if min_cluster:
                min_cluster.update_stat_w_point(point)
                self.cluster_result[p_idx] = min_cluster.cluster_idx  # update cluster result for this point
                self.retained_set.remove(p_idx)  # remove this point from the retained set

    def assign_rs_to_cs(self):
        rs = copy.copy(self.retained_set)
        for p_idx in rs:
            point = self.dt_idx_dict[p_idx]
            min_cluster = find_nearest_cluster_for_point(point, self.compress_set)
            if min_cluster:
                min_cluster.update_stat_w_point(point)
                self.cluster_result[p_idx] = min_cluster.cluster_idx  # update the clustering result for this point
                self.retained_set.remove(p_idx)  # remove this point from the retained set

    def merge_cs(self):

        candidates = []
        candidate_pairs = []

        for i in range(len(self.compress_set) - 1):
            cluster = self.compress_set[i]
            if i not in candidates and i < len(self.compress_set):
                cluster_set = self.compress_set[(i + 1):]
                min_dis, min_cluster, min_cluster_idx = find_nearest_cluster_for_cluster(cluster, cluster_set)
                min_cluster_idx += i + 1
                if min_cluster_idx not in candidates and min_dis < 2 * math.sqrt(self.n_attr):
                    candidates.append(i)
                    candidates.append(min_cluster_idx)
                    candidate_pairs.append([min_cluster_idx, i])

        cs = [c for i, c in enumerate(self.compress_set) if i not in candidates]
        for pair in candidate_pairs:
            cluster1 = self.compress_set[pair[0]]
            cluster2 = self.compress_set[pair[1]]
            cluster1.update_stat_w_cluster(cluster2)
            cs.append(cluster1)
            self.update_cluster_result(cluster1, cluster2)

        self.compress_set = cs

    def generate_clusters(self, dt_idx, k):

        if len(dt_idx) < k:
            return [], dt_idx

        dt = [self.dt_idx_dict[i] for i in dt_idx]
        scl_dt = standard_scaler(dt)
        #step 6
        labels, _ = get_k_means_label(scl_dt, k, max_iter=100)
        singles, clusters = [], []
        for n_cluster in range(k):
            label_idx = [i for i, label in enumerate(labels) if label == n_cluster]
            if len(label_idx) < 1:
                continue
            elif len(label_idx) == 1:
                singles.append(dt_idx[label_idx[0]])  # if the cluster has only one value, it will go to rs
            else:
                new_cluster = CLuster(self.cur_cluster_idx, self.n_attr)  # build a new cluster
                self.cur_cluster_idx += 1   # update current number of clusters in the BFR
                for i in label_idx:
                    new_cluster.update_stat_w_point(dt[i])
                    self.cluster_result[dt_idx[i]] = new_cluster.cluster_idx  # update cluster result for this point
                clusters.append(new_cluster)
        return clusters, singles

    def finish(self):
        remain_cs = []
        for i in range(len(self.compress_set)):
            cluster = self.compress_set[i]
            min_dis, min_cluster, _ = find_nearest_cluster_for_cluster(cluster, self.discard_set)
            if min_cluster and min_dis < 2 * math.sqrt(self.n_attr):
                min_cluster.update_stat_w_cluster(cluster)
                self.update_cluster_result(min_cluster, cluster)
            else:
                remain_cs.append(cluster)
        self.compress_set = remain_cs

    def update_cluster_result(self, c1, c2):
        c1_idx = c1.cluster_idx
        c2_idx = c2.cluster_idx
        cluster2_instance = [k for k, v in self.cluster_result.items() if v == c2_idx]
        for i in cluster2_instance:
            self.cluster_result[i] = c1_idx
