
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from main import apply_secret_stat


def test_secret_agg():
    # test pour vérifier que les accents et les colonnes sont bient traité
    test = {"REGION": [1, 4, 4, 4, 4, 4],
            "TYPE_ENTREPRISE": ["PME", "GE", "PME", "PME", "PME", "PME"],
            "ENTREPRISE": ["E", "E", "E", "E", "E", "ME"],
            "argent1": [0, 0, 0, 0, 0, 999999],
            "argent2": [10, 10, 10, 10, 10, 11]
            }
    expected = {  # REGION, TYPE_ENTREPRISE
                'REGION': [np.nan, np.nan, np.nan],
                'TYPE_ENTREPRISE': [np.nan, np.nan, np.nan],
                'argent1_max': [np.nan, np.nan, np.nan],
                'argent1_sum': [np.nan, np.nan, np.nan],
                'argent1_count': [np.nan, np.nan, np.nan],
                'argent1_max_percentage': [np.nan, np.nan, np.nan],
                'argent2_max': [np.nan, np.nan, np.nan],
                'argent2_sum': [np.nan, np.nan, np.nan],
                'argent2_count': [np.nan, np.nan, np.nan],
                'argent2_max_percentage': [np.nan, np.nan, np.nan],
                'ENTREPRISE': [np.nan, np.nan, np.nan]
               }
    expected2 = {  # REGION, ENTREPRISE
                'REGION': [np.nan, 4, np.nan],
                'ENTREPRISE': [np.nan, 'E', np.nan],
                'argent1_max': [np.nan, 0.0, np.nan],
                'argent1_sum': [np.nan, 0.0, np.nan],
                'argent1_count': [np.nan, 4, np.nan],
                'argent1_max_percentage': [np.nan, np.nan, np.nan],
                'argent2_max': [np.nan, 10.0, np.nan],
                'argent2_sum': [np.nan, 40.0, np.nan],
                'argent2_count': [np.nan, 4.0, np.nan],
                'argent2_max_percentage': [np.nan, 25.0, np.nan],
                'TYPE_ENTREPRISE': [np.nan, 'GE', np.nan]
    }
    expected = pd.DataFrame(expected)
    expected['ENTREPRISE'] = expected['ENTREPRISE'].astype(object)
    expected['TYPE_ENTREPRISE'] = expected['TYPE_ENTREPRISE'].astype(object)
    expected2 = pd.DataFrame(expected2)
    expected2['ENTREPRISE'] = expected2['ENTREPRISE'].astype(object)
    expected2['TYPE_ENTREPRISE'] = expected2['TYPE_ENTREPRISE'].astype(object)

    exp3 = {
        ('REGION', 'TYPE_ENTREPRISE'): expected,
        ('REGION', 'ENTREPRISE'): expected2
    }

    test = pd.DataFrame(test)
    group = [
                ["REGION", "TYPE_ENTREPRISE"],
                ["REGION", "ENTREPRISE"]
            ]
    col_secret = ("argent1", "argent2")

    df_dict = apply_secret_stat(columns_to_check=col_secret,
                                dataframe=test,
                                list_aggregation=group)
    for data_frame in df_dict:
        assert_frame_equal(df_dict[data_frame].reset_index(drop=True), exp3[data_frame].reset_index(drop=True))


if __name__ == "__main__":
    test_secret_agg()
