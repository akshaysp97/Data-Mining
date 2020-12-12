from sklearn import preprocessing
from sklearn.cluster import KMeans
import math


MAX_DISTANCE = 10000.0


def find_nearest_cluster_for_point(point, cluster_set):
    min_dis, min_cluster = MAX_DISTANCE, None
    for cluster in cluster_set:
        this_dis, _ = mah_distance_cluster_point(cluster, point)
        if this_dis < min_dis and this_dis < 2 * math.sqrt(len(point)):
            min_dis = this_dis
            min_cluster = cluster
    return min_cluster


def find_nearest_cluster_for_cluster(cluster, cluster_set):
    min_dis, min_cluster, min_cluster_idx = MAX_DISTANCE, None, None
    for i in range(len(cluster_set)):
        cluster2 = cluster_set[i]
        this_dis, _ = mah_distance_cluster_cluster(cluster2, cluster)
        if this_dis < min_dis:
            min_dis, min_cluster, min_cluster_idx = this_dis, cluster2, i
    return min_dis, min_cluster, min_cluster_idx


"""
    Calculates Mahalanobis distance by using sufficient statistic
    sigma = SUMSQi/N-(SUMi/N)^2
    deviation = sqrt(sigma)

"""


def mah_distance_cluster_point(cluster, point):

    cluster_stat = cluster.get_stat()
    cluster_sum = cluster_stat.get_sum()
    cluster_sumsq = cluster_stat.get_sumsq()
    cluster_centroid = cluster_stat.get_centroid()
    cluster_n_points = cluster_stat.get_n_points()
    n_points = cluster_n_points + 1

    assert len(cluster_centroid) == len(point)
    n_attr = len(point)
    distance = 0.0
    msigma = 0.0

    for i in range(n_attr):
        sigma = (cluster_sumsq[i] + math.pow(point[i], 2)) / n_points \
                - math.pow(((cluster_sum[i] + point[i]) / n_points), 2)
        if sigma != 0.0:
            distance += math.pow(((point[i] - cluster_centroid[i]) / math.sqrt(sigma)), 2)
        msigma += math.sqrt(sigma)
    return math.sqrt(distance), msigma


def mah_distance_cluster_cluster(cluster1, cluster2):

    distance = 0.0

    cluster_stat1 = cluster1.get_stat()
    cluster_sum1 = cluster_stat1.get_sum()
    cluster_sumsq1 = cluster_stat1.get_sumsq()
    cluster_centroid1 = cluster_stat1.get_centroid()
    cluster_n_points1 = cluster_stat1.get_n_points()

    cluster_stat2 = cluster2.get_stat()
    cluster_centroid2 = cluster_stat2.get_centroid()
    cluster_n_points2 = cluster_stat2.get_n_points()

    n_points = cluster_n_points1 + cluster_n_points2

    assert len(cluster_centroid1) == len(cluster_centroid2)
    n_attr = len(cluster_centroid1)
    msigma = 0

    for i in range(n_attr):
        sigma = (cluster_sumsq1[i] + math.pow(cluster_centroid2[i], 2)) / n_points \
                - math.pow((cluster_sum1[i] + cluster_centroid2[i]) / n_points, 2)
        if sigma != 0.0:
            distance += math.pow(((cluster_centroid2[i] - cluster_centroid1[i]) / math.sqrt(sigma)), 2)
        msigma += math.sqrt(sigma)
    return math.sqrt(distance), msigma


def get_k_means_label(data, k, max_iter=300):
    assert k <= len(data)
    km = KMeans(n_clusters=k, max_iter=max_iter).fit(data)
    labels = km.labels_
    ctr = km.cluster_centers_
    return labels, ctr


def standard_scaler(data):
    scl = preprocessing.StandardScaler().fit(data)
    return scl.transform(data)
