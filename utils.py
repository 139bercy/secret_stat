import os
import pandas as pd


def max_percentage(series: pd.Series):
    # Get the percentage of the maximum contributor of the series' sum.
    max_val = series.sum()
    return round(max(series.transform(lambda x: x * 100 / max_val)), 2)


def save_values(path: str, my_dict: dict):
    for df_name in my_dict:
        my_dict[df_name].to_csv(os.path.join(path, "agg_" + "_".join(df_name) + ".csv"),
                           sep=';',
                           index=False,
                           encoding='utf-8')


def dataframe_3D_to_2D(dict3D: dict, columns_secret: list) -> dict:
    final_dict = {}
    for df_key in dict3D:
        df3D = dict3D[df_key]
        df_new_column = get_col_name_2D(df3D, columns_secret)
        df2D = df3D.copy()
        df2D.columns = df2D.columns.droplevel(1)
        df2D = df2D.join(df_new_column)
        for secret_col in columns_secret:
            df2D = df2D.drop([secret_col], axis=1)
        # at this point I have a 2D dataframe need to append to dict
        final_dict[df_key] = df2D

    return final_dict


def get_col_name_2D(df3D: pd.DataFrame, columns_secret: list) -> pd.DataFrame:
    count_option_list = ["count", "max", "sum", "max_percentage"]

    for secret_column in columns_secret:  # secret_column = nom colonne à rajouter avec _count, _max etc...
        for option in count_option_list:
            list_secret_col = df3D[secret_column][option].tolist()
            col_name = secret_column + "_" + option
            dict_col = {col_name: list_secret_col}
            df1 = pd.DataFrame(dict_col)
            if option == count_option_list[0]:
                df = df1
            else:
                df = df.join(df1)
        if secret_column == columns_secret[0]:
            df_new_column = df
        else:
            df_new_column = df_new_column.join(df)

    return df_new_column


LIST_FUNCTIONS = [('max', max),
                  ('sum', sum),
                  ('count', 'count'),
                  ('count', sum)]

MEASURE_TYPES = {
    'max',  # mandatory for secret checking
    'sum',  # mandatory for secret checking
    'count'  # mandatory for secret checking
}
