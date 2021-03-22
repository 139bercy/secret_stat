import pandas as pd
from aggregation import Version4SafeAggregation


def apply_secret_stat(dataframe: pd.DataFrame,
                      columns_to_check: list,
                      list_aggregation: list,
                      dominance: int = 85,
                      frequence: int = 3) -> dict:

    specific_aggregator = Version4SafeAggregation(dataframe, columns_to_check, list_aggregation, dominance, frequence)

    final_masked_dict = specific_aggregator.aggregateFactory()

    return final_masked_dict


if __name__ == "__main__":
    gb = [
        ["REGION", "ENTREPRISE"],
        ["REGION", "TYPE_ENTREPRISE"]
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

    x = apply_secret_stat(dataframe=test,
                          columns_to_check=col_secret,
                          list_aggregation=gb)
