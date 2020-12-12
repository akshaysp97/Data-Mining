import sys

from bfr import BFR
from sklearn.metrics.cluster import normalized_mutual_info_score


def output_results(bfr_ins, n_sample, last_round=False):

    n_discard_points = 0
    for c in bfr_ins.discard_set:
        n_discard_points += c.get_stat().get_n_points() - 1

    n_compression_points = 0
    for c in bfr_ins.compress_set:
        n_compression_points += c.get_stat().get_n_points()

    print('Number of clusters in the discard set: {}. Total discard points: {}. '\
          'Number of clusters in the compression set: {}. Total compression points: {}. '\
          'Number of points in the retained set: {}.'\
          .format(len(bfr_ins.discard_set), n_discard_points,
                  len(bfr_ins.compress_set), n_compression_points,
                  len(bfr_ins.retained_set)))

    if last_round:
        print('Percentage of discard points: {}%'.format(round(n_discard_points / n_sample, 3) * 100))

    return [n_discard_points, len(bfr_ins.compress_set), n_compression_points, len(bfr_ins.retained_set)]


def bfr_main(n_cluster, dt, dt_idx_dict):

    output_info = []
    cluster_result = {i: -1 for i in range(len(dt))}
    n_sample = len(dt)

    pct = 0.2
    n_init_data = int(n_sample * pct)
    n_attr = len(dt[0])
    bfr_ins = BFR(n_attr, n_cluster, dt_idx_dict, cluster_result)

    init_idx = [k for k, v in dt_idx_dict.items() if k < n_init_data]
    bfr_ins.init(init_idx)
    output_info.append(output_results(bfr_ins, n_sample))

    start = int(n_sample * pct)
    end = start + int(n_sample * pct)

    while start < n_sample:
        data_idx = [k for k, v in dt_idx_dict.items() if start <= k < end]
        bfr_ins.bfr_main(data_idx)
        start = end
        end = start + int(n_sample * pct)
        output_info.append(output_results(bfr_ins, n_sample))

    bfr_ins.finish()
    res = output_results(bfr_ins, n_sample, last_round=True)
    return bfr_ins, output_info


if __name__ == '__main__':

    input_data = sys.argv[1]
    n_cluster = int(sys.argv[2])
    output_path = sys.argv[3]

    f = open(input_data, 'r')
    dt, cluster_gt = [], []
    for line in f:
        line_data = line.strip().split(',')
        dt.append([float(d) for i, d in enumerate(line_data) if i != 0 and i != 1])
        cluster_gt.append(int(line_data[1]))
    f.close()


    n_dim = len(dt[0])

    dt_idx_dict = {i: point for i, point in enumerate(dt)}
    bfr_ins, output_info = bfr_main(n_cluster, dt, dt_idx_dict)

    # assign -1 to the outliers
    cluster_result = bfr_ins.cluster_result
    for k, v in cluster_result.items():
        if v >= n_cluster:
            cluster_result[k] = -1
    rt = [[k, dt_idx_dict[k], v, cluster_gt[k]] for k, v in cluster_result.items()]

    rt1 = [i[-2] for i in rt]
    rt2 = [i[-1] for i in rt]
    norm_acc = normalized_mutual_info_score(rt1, rt2)
    print('Normalized_mutual_info_score: ' + str(norm_acc))

    # get centroids from the results
    rt_centroids = []
    for each in bfr_ins.discard_set:
        stat = each.get_stat()
        print(stat.get_centroid())
        rt_centroids.append(stat.get_centroid())

    file = open(output_path, 'w')
    file.write('The intermediate results:\n')

    for i, res in enumerate(output_info):
        s = [str(d) for d in res]
        file.write('Round {}: {}'.format(i, ','.join(s)) + '\n')

    file.write('The clustering results:\n')
    for i, line in enumerate(rt):
        s = str(line[0]) + ',' + str(line[2])
        file.write(s + '\n')
