import json
import pandas as pd
from utils import save_values, dataframe_3D_to_2D
from aggregation import SafeAgregation


def apply_secret_stat(group_by: list,
                      columns_apply_secret: list,
                      column_to_check: str,
                      data_path: str = None,
                      sep: str = "|",
                      dataframe: pd.DataFrame = None,
                      export_to_csv: bool = False,
                      path_to_export: str = "./") -> dict:
    if data_path is None:
        if dataframe is None:
            exit("specify data in order to process")
        df_entreprises = dataframe
    else:
        if dataframe:
            exit("To many data provided")

        with open("config.json") as f:
            config = json.load(f)
        # Importing Dataset
        df_entreprises = pd.read_csv(data_path, encoding='utf-8', sep=sep)

    # Instanciate class
    specific_aggregator = SafeAgregation(common_column=column_to_check, columns_apply_secret=columns_apply_secret)

    # Test the multiple aggregation
    final_masked_dict = specific_aggregator.specific_aggregator_factory(df_entreprises, group_by,
                                                                        columns_apply_secret)

    final_2D_masked_dict = dataframe_3D_to_2D(final_masked_dict, columns_apply_secret)

    if export_to_csv:
        save_values(path_to_export, final_2D_masked_dict)

    return final_2D_masked_dict


if __name__ == "__main__":
    gb = [
        ("REGION", "ENTREPRISE"),
        ("REGION", "TYPE_ENTREPRISE")
    ]
    col_secret = [
        "argent2",
        "argent1"
    ]
    test = {"REGION": [1, 4, 4, 4, 4, 4],
            "TYPE_ENTREPRISE": ["PME", "GE", "PME", "PME", "PME", "PME"],
            "ENTREPRISE": ["E", "E", "E", "E", "E", "ME"],
            "argent1": [0, 0, 0, 0, 0, 999999],
            "argent2": [10, 10, 10, 10, 10, 11]
            }
    test = pd.DataFrame(test)
    self = "self"
    x = apply_secret_stat(group_by=gb,
                          columns_apply_secret=col_secret,
                          column_to_check="REGION",
                          dataframe=test)
    # export_to_csv = True,
