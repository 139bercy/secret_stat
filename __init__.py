import json
import pandas as pd
import os
from utils import save_values
from aggregation import Version3SafeAggregation


def apply_secret_stat(DATA_PATH, GroupBy, ImportantColumns, separator="|"):
    with open("config.json") as f:
        config = json.load(f)
    # Importing Dataset
    df_entreprises = pd.read_csv(DATA_PATH, encoding='utf-8', sep=separator)

    """
    # List of keys to group by
    GroupBy = [
        ("CODE_REGION", "CODE_DEPARTEMENT"),
        ("CODE_REGION", "TYPE_ENTREPRISE"),
        ("CODE_REGION", "MESURE"),
        ("CODE_REGION", "FILIÈRE"),
        ("MESURE", "FILIÈRE"),
        ("MESURE", "TYPE_ENTREPRISE"),
    ]
    """

    # Instanciate class
    specific_aggregator = Version3SafeAggregation()

    # Test the multiple aggregation
    version_3 = specific_aggregator.perform_multiple_safe_aggregation(df_entreprises, GroupBy)

    dict_masked_secondary = specific_aggregator.check_and_apply_secondary_secret(version_3,
                                                                                 GroupBy,
                                                                                 verbose=True)
    final_masked_dict = specific_aggregator.mask_values(dict_masked_secondary)

    save_values(os.path.join(config["OUTPUT_DATA_PATH"], config["FILE_NAME"]), final_masked_dict)


if __name__ == "__main__":

    listo = [
        ("CODE_REGION", "CODE_DEPARTEMENT"),
        ("CODE_REGION", "TYPE_ENTREPRISE"),
        ("CODE_REGION", "MESURE"),
        ("CODE_REGION", "FILIÈRE"),
        ("MESURE", "FILIÈRE"),
        ("MESURE", "TYPE_ENTREPRISE"),
    ]
    apply_secret_stat("/home/guillaume/Documents/planRelance/db-planr/scripts_python/safe_aggregation/decide_planr.csv",
                      listo,
                      "ee",
                      "|"
                      )
