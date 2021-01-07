import json
import pandas as pd
import os
from utils import save_values
from aggregation import Version3SafeAggregation


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
        ("CODE_REGION", "CODE_DEPARTEMENT"),
        ("CODE_REGION", "TYPE_ENTREPRISE"),
        ("CODE_REGION", "MESURE"),
        ("CODE_REGION", "FILIÈRE"),
        ("MESURE", "FILIÈRE"),
        ("MESURE", "TYPE_ENTREPRISE"),
    ]
    col_secret = [
        "MONTANT_INVESTISSEMENT",
        "MONTANT_PARTICIPATION_ETAT"
    ]

    apply_secret_stat(group_by=gb,
                      columns_apply_secret=col_secret,
                      column_to_check="CODE_REGION",
                      data_path="/home/guillaume/Documents/planRelance/db-planr/scripts_python/safe_aggregation/decide_planr.csv",
                      export_to_csv=True,
                      sep="|")
