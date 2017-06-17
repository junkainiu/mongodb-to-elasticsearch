
def list_sort(items, order=None):
    res = []
    if not order:
        order = []
    for item in order:
        if item in items:
            res.append(item)
    for item in items:
        if item not in res:
            res.append(item)
    return res
