import json

import pytest
import pandas as pd
from pandas._testing import assert_frame_equal
from main import SecretStat


def test_format_dataframe():
    # test pour vérifier que les accents et les colonnes sont bient traité
    test = {"REGION": [1, 4, 4, 4, 4, 4],
            "TYPE_ENTREPRISE": ["PME", "GE", "PME", "PME", "PME", "PME"],
            "ENTREPRISE": ["E", "E", "E", "E", "E", "ME"],
            "argent1": [0, 0, 0, 0, 0, 999999],
            "argent2": [10, 10, 10, 10, 10, 11]
            }

    expected = {
                ('REGION', ''): [1, 4, 4],
                ('TYPE_ENTREPRISE', ''): ["PME", "GE", "PME"],
                ('argent1', 'sum'): ["NaT", "NaT", "NaT"],
                ('argent2', 'sum'): ["NaT", "NaT", "NaT"]
               }
    expected2 = {
        ('REGION', ''): [1, 4, 4],
        ('TYPE_ENTREPRISE', ''): ["PME", "GE", "PME"],
        ('argent1', 'sum'): ["NaT", "NaT", "NaT"],
        ('argent2', 'sum'): ["NaT", "NaT", "NaT"]
    }
    expected = pd.DataFrame(expected)
    expected2 = pd.DataFrame(expected2)

    exp3 = {
        ('REGION', 'TYPE_ENTREPRISE'): expected,
        ('REGION', 'ENTREPRISE'): expected2
    }

    pd.concat(exp3)
    exp3 = pd.DataFrame(exp3)
    print(exp3)
    test = pd.DataFrame(test)
    group = [
                ("REGION", "TYPE_ENTREPRISE"),
                ("ENTREPRISE", "REGION")
            ]
    df = SecretStat.apply_secret_stat(group_by=group,
                                      columns_apply_secret=("argent1", "argent2"),
                                      column_to_check="REGION",
                                      export_to_csv=True,
                                      dataframe=test)


    dictA_str = json.dumps(str(df), sort_keys=True)
    dictB_str = json.dumps(str(exp3), sort_keys=True)
    print(dictA_str)
    print(dictB_str)
    assert dictA_str == dictB_str

    #assert_frame_equal(df, df, check_names=True)
