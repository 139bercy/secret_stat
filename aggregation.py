import json
import numpy as np
from utils import max_percentage, MEASURE_TYPES
import pandas as pd


class Version4SafeAggregation:

    def __init__(self, dataframe: pd.DataFrame, columns_to_check: list, list_aggregation: list, dominance, frequence, *args, **kwargs):
        self.dataframe = dataframe
        self.frequence = frequence
        self.dominance = dominance
        self.columns_to_check = columns_to_check
        self.group_by = list_aggregation
        self.measure_types = MEASURE_TYPES
        self.measure_types.append(max_percentage)
        self.dict_aggreg = self._create_dict_aggregation(columns_to_check)
        with open("config.json") as f:
            config = json.load(f)
        self.rules_list = config["RULES"]

    def _create_dict_aggregation(self, list_targets: list) -> dict:
        self.dict_aggreg = {}
        final_dict = {target: self.measure_types for target in list_targets}
        return final_dict

    def aggregateFactory(self) -> dict:  # pour chaque clef créer un dataframe avec les données censurées
        dict_df = {}
        for gb_key in self.group_by:
            dict_df[tuple(gb_key)] = (self.safe_aggregate(self.dataframe, gb_key))

        return dict_df

    def safe_aggregate(self, df: pd.DataFrame, gb_keys: list) -> pd.DataFrame:
        self.prepare_aggregate()
        aggregated_df_3D = df.groupby(gb_keys, as_index=True).agg(self.dict_aggreg).reset_index(level=0, drop=True)
        aggregated_df = self.dataframe_3D_to_2D(aggregated_df_3D)
        safe_df = self.check_secret(aggregated_df, gb_keys)
        safe_df = self.reorder_columns(safe_df, gb_keys)
        return safe_df

    def prepare_aggregate(self):
        for column in self.dataframe:
            if column not in self.columns_to_check:
                self.dict_aggreg[column] = "first"

    def dataframe_3D_to_2D(self, df3D: pd.DataFrame) -> pd.DataFrame:
        df3D.columns = ['_'.join(col) for col in df3D.columns.values]
        df2D = df3D.copy()
        for column in df2D:  # this loop remove the _first after the columns names
            if "_first" in column:
               df2D = df2D.rename(columns={column: column.replace('_first', '')})
        return df2D

    def check_secret(self, df: pd.DataFrame, gb_key: list) -> pd.DataFrame:
        df_gb_key_keep = df.copy()
        df_gb_key_keep = df_gb_key_keep.filter(gb_key)
        for col_secret in self.columns_to_check:
            df = self.check_max_percent(df, col_secret)
            df = self.check_count(df, col_secret)
        df = df.drop(gb_key, axis=1)
        df = pd.concat([df_gb_key_keep, df], axis=1)
        return df

    def check_max_percent(self, df: pd.DataFrame, col_secret: str) -> pd.DataFrame:
        name = "_max_percentage"
        col_secret = col_secret + name
        df_percent = df.copy()
        df_percent.loc[df_percent[col_secret] >= self.dominance] = np.nan
        return df_percent

    def check_count(self, df: pd.DataFrame, col_secret: str) -> pd.DataFrame:
        df_count = df.copy()
        name = "_count"
        col_secret = col_secret + name
        df_count.loc[df_count[col_secret] <= self.frequence] = np.nan
        return df_count

    def reorder_columns(self, df, firsts_col):
        cols = list(df.columns.values)
        for i in firsts_col:
            cols.remove(i)
            cols.insert(0, i)
        cols.remove(firsts_col[0])
        cols.insert(0, firsts_col[0])
        df = df[cols]
        reorder_df = df
        return reorder_df
