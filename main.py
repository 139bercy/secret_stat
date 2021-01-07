import json
import pandas as pd
from utils import save_values
from aggregation import Version3SafeAggregation


class SecretStat:

    def apply_secret_stat(group_by,
                          columns_apply_secret,
                          column_to_check,
                          data_path=None,
                          sep="|",
                          dataframe=None,
                          export_to_csv=None,
                          path_to_export="./"):
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
        specific_aggregator = Version3SafeAggregation(column_to_check, secret_columns=columns_apply_secret)

        # Test the multiple aggregation
        final_masked_dict = specific_aggregator.specific_aggregator_factory(df_entreprises, group_by, columns_apply_secret)

        if export_to_csv:
            save_values(path_to_export, final_masked_dict)

        return final_masked_dict

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
    x = SecretStat.apply_secret_stat(group_by=gb,
                                 columns_apply_secret=col_secret,
                                 column_to_check="REGION",
                                 dataframe=test,
                                 export_to_csv=True,
                                 sep="|")
    for dict in x:
        print(dict)
        for col in x[dict]:
            print(col)
            print(x[dict][col])

    print("hello")
