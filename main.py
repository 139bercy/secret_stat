import json
import pandas as pd
from utils import save_values, dataframe_3D_to_2D
from aggregation import Version3SafeAggregation, Version4SafeAggregation


def apply_secret_stat(dataframe: pd.DataFrame,
                      columns_to_check: list,
                      columns_to_mask: list) -> dict:

    specific_aggregator = Version4SafeAggregation(dataframe, columns_to_check, columns_to_mask)

    final_masked_dict = specific_aggregator.aggregateFactory()

    final_2D_masked_dict = dataframe_3D_to_2D(final_masked_dict, columns_apply_secret)

    return final_2D_masked_dict


if __name__ == "__main__":
    gb = [
        ("REGION", "ENTREPRISE"),
        ("REGION", "TYPE_ENTREPRISE")
    ]
    col_secret = [
        "argent2"
    ]
    test = {"REGION": [1, 4, 4, 4, 4, 4],
            "TYPE_ENTREPRISE": ["PME", "GE", "PME", "PME", "PME", "PME"],
            "ENTREPRISE": ["E", "E", "E", "E", "E", "ME"],
            "argent1": [0, 0, 0, 0, 0, 999999],
            "argent2": [10, 10, 10, 10, 10, 11]
            }
    test = pd.DataFrame(test)
    self = "self"
    x = apply_secret_stat(dataframe=test,
                          columns_to_check=col_secret,
                          columns_to_mask=gb)
    # export_to_csv = True,
