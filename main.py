import pandas as pd
from utils import save_values, dataframe_3D_to_2D, check_user_input
from aggregation import Version3SafeAggregation


def apply_secret_stat(group_by: list,
                      columns_apply_secret: list,
                      column_to_check: str = None,
                      data_path: str = None,
                      sep: str = "|",
                      dataframe: pd.DataFrame = None,
                      export_to_csv: bool = False,
                      path_to_export: str = "./") -> dict:

    df = check_user_input(data_path, dataframe, sep, column_to_check)

    # Instanciate class
    specific_aggregator = Version3SafeAggregation(column_to_check, secret_columns=columns_apply_secret)

    # Test the multiple aggregation
    final_masked_dict = specific_aggregator.specific_aggregator_factory(df, group_by,
                                                                        columns_apply_secret)

    if export_to_csv:
        save_values(path_to_export, final_masked_dict)

    final_2D_masked_dict = dataframe_3D_to_2D(final_masked_dict, columns_apply_secret)

    return final_2D_masked_dict
