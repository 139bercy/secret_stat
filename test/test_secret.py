
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from secret_statistic.secret_agg import apply_secret_stat


def test_secret_agg():
    # test pour vérifier que les accents et les colonnes sont bient traité
    test = {"REGION": [1, 4, 4, 4, 4, 4],
            "TYPE_ENTREPRISE": ["PME", "GE", "PME", "PME", "PME", "PME"],
            "ENTREPRISE": ["E", "E", "E", "E", "E", "ME"],
            "argent1": [0, 0, 0, 0, 0, 999999],
            "argent2": [10, 10, 10, 10, 10, 11]
            }
    expected = {  # REGION, TYPE_ENTREPRISE
                'REGION': [1, 4, 4],
                'TYPE_ENTREPRISE': ["PME", "GE", "PME"],
                'argent1_count': [np.nan, np.nan, np.nan],
                'argent1_max': [np.nan, np.nan, np.nan],
                'argent1_sum': [np.nan, np.nan, np.nan],
                'argent1_max_percentage': [np.nan, np.nan, np.nan],
                'argent2_count': [np.nan, np.nan, 4.0],
                'argent2_max': [np.nan, np.nan, 11.0],
                'argent2_sum': [np.nan, np.nan, 41.0],
                'argent2_max_percentage': [np.nan, np.nan, 26.83]
               }
    expected2 = {  # REGION, ENTREPRISE
                'REGION': [1, 4, 4],
                'ENTREPRISE': ["E", "E", "ME"],
                'argent1_count': [np.nan, np.nan, np.nan],
                'argent1_max': [np.nan, np.nan, np.nan],
                'argent1_sum': [np.nan, np.nan, np.nan],
                'argent1_max_percentage': [np.nan, np.nan, np.nan],
                'argent2_count': [np.nan, 4.0, np.nan],
                'argent2_max': [np.nan, 10.0, np.nan],
                'argent2_sum': [np.nan, 40.0, np.nan],
                'argent2_max_percentage': [np.nan, 25.0, np.nan]
    }
    expected = pd.DataFrame(expected)
    expected2 = pd.DataFrame(expected2)

    exp3 = {
        ('REGION', 'TYPE_ENTREPRISE'): expected,
        ('REGION', 'ENTREPRISE'): expected2
    }

    test = pd.DataFrame(test)
    group = [
                ("REGION", "TYPE_ENTREPRISE"),
                ("REGION", "ENTREPRISE")
            ]
    col_secret = ("argent1", "argent2")

    df_dict = apply_secret_stat(group_by=group,
                                columns_apply_secret=col_secret,
                                column_to_check="REGION",
                                export_to_csv=True,
                                dataframe=test)
    for data_frame in df_dict:
        assert_frame_equal(df_dict[data_frame], exp3[data_frame])

if __name__ == "__main__":
    test_secret_agg()
