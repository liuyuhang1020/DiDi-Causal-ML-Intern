def get_preprocess_conf(configs):
    res = []
    for row in configs:
        if row[2] == 'self':
            row = tuple(list(row[:2]) + [row[0]] + list(row[3:]))
        res.append(row)
    return res


def get_sparse_conf(configs):
    res = []
    for row in configs:
        if len(row) == 3:
            row = tuple(list(row) + [row[0]])
        res.append(row)
    return res
