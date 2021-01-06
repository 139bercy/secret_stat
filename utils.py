import itertools

def max_percentage(series):
    # Get the percentage of the maximum contributor of the series' sum.
    max_val = series.sum()
    return round(max(series.transform(lambda x: x * 100 / max_val)), 2)


def save_values(name, my_dict):
    for k, v in my_dict.items():
        v.to_csv("agg_" + "_".join(k) + '.csv', sep=';',
                 index=False,
                 encoding='utf-8')


LIST_FUNCTIONS = [('max', max),
                  ('sum', sum),
                  ('count', 'count'),
                  ('count', sum)]

MEASURE_TYPES = {
    'max',  # mandatory for secret checking
    'sum',  # mandatory for secret checking
    'count'  # mandatory for secret checking
}
