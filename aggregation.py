import json
import numpy as np
from utils import max_percentage, LIST_FUNCTIONS, MEASURE_TYPES
import pandas as pd


class SafeAgregation():
    """ Perform aggregation and primary secret check.

    Attributes :
    - relevant_column : list of columns on which to perform aggregation
    - measure_types : set of functions for groupby
    - rules_dict : dictionnary of rules with rule name and rule threshold
    - dict_aggreg : dictionnary of aggregation for groupby


    Public methods available :
    - aggregate : perform aggregation
    - safe_aggregate : perform aggregation and check primary secret
    - perform_multiple_safe_aggregation : perform aggregation for list of groups of columns to groupby
    - get_sum_masked : get information on the number of cells and the value being masked
    - mask_values : perform masking of the cells for primary secret
    """
    default_measure_types = MEASURE_TYPES.union({max_percentage})

    def __init__(self, columns_apply_secret, measure_types=default_measure_types):
        with open("config.json") as f:
            config = json.load(f)
        self.relevant_column = columns_apply_secret
        self.measure_types = measure_types
        self.rules_list = config["RULES"]
        self.dict_aggreg = self._create_dict_aggregation(columns_apply_secret)

    def specific_aggregator_factory(self, df: pd.DataFrame, group_by: list, columns_apply_secret: list) -> pd.DataFrame:
        version_3 = self.perform_multiple_safe_aggregation(df, group_by)
        dict_masked_secondary = self.check_and_apply_secondary_secret(version_3,
                                                                      group_by,
                                                                      columns_apply_secret,
                                                                      verbose=True)
        final_masked_dict = self.mask_values(dict_masked_secondary)
        return final_masked_dict

    def set_measures(self, new_measures):
        self.measure_types = new_measures

    def _create_dict_aggregation(self, list_targets: list) -> dict:
        final_dict = {target: self.measure_types for target in list_targets}
        return final_dict

    def _create_error_message(self, df: pd.DataFrame, rule):
        with open("config.json") as f:
            config = json.load(f)
        error_cases = df[df < rule['THRESHOLD']] if rule['MIN_THRESH'] else df[df > rule['THRESHOLD']]
        error_message = [config["ERROR_MESSAGE"].format(rule['RULE_NAME'])]
        missing_val = (
            ['{} : {}  sur {} \n'.format(x, y, z) for (x, y, z) in
             zip(error_cases.count().index.get_level_values(0).values,
                 error_cases.count().values,
                 [df.shape[0]] * 2)])
        error_message += missing_val
        return ''.join(error_message)

    def _check_primary_secret(self, df: pd.DataFrame, verbose: bool) -> pd.DataFrame:
        with open("config.json") as f:
            config = json.load(f)

        safe_df = df.copy()

        for column_name in self.relevant_column:
            safe_df[column_name + '_keep'] = True

        # Check all rules
        for rule in self.rules_list:
            partial_df = safe_df.iloc[:, safe_df.columns.get_level_values(1) == rule['COLUMN_NAME']]
            condition_df = (partial_df > rule['THRESHOLD']) if rule['MIN_THRESH'] else (partial_df < rule['THRESHOLD'])

            for column_name in self.relevant_column:
                safe_df[column_name + '_keep'] = [(prev_val & new_val[0]) for (prev_val, new_val) in
                                                  zip(safe_df[column_name + '_keep'],
                                                      condition_df[column_name].values)]
            if verbose:
                if condition_df.any().any():
                    print(self._create_error_message(partial_df, rule))
                else:
                    print(config["ALL_FINE_MESSAGE"].format(partial_df.shape[0], rule['RULE_NAME']))
        return safe_df

    def aggregate(self, df: pd.DataFrame, gb_keys: list):
        aggregated_df = df.groupby(list(gb_keys), as_index=False).agg(self.dict_aggreg)
        return aggregated_df

    def safe_aggregate(self, df: pd.DataFrame, gb_keys: list, verbose=False) -> pd.DataFrame:
        aggregated_df = df.groupby(list(gb_keys), as_index=False).agg(self.dict_aggreg)
        safe_df = self._check_primary_secret(aggregated_df, verbose)
        return safe_df

    def perform_multiple_safe_aggregation(self, df: pd.DataFrame, list_gb_keys: list) -> dict:
        """ Performs multiple aggregation and primary secret check.

        Input parameters :
        - df : dataframe of data to be aggregated
        - list_gb_keys : list of groups of columns, used in groupby

        Output ;
        - dict_df : dictionnary where each element of list_gb_keys is a key,
                    and the value is the corresponding aggregated dataframe
        """
        dict_df = {}
        for gb_key in list_gb_keys:
            dict_df[gb_key] = (self.safe_aggregate(df, gb_key))
        return dict_df

    def get_sum_masked(self, dict_df):
        for k, v in dict_df.items():
            print(k)
            for column_name in self.relevant_column:
                print(column_name)
                print('Number of cells : {}'.format(v[~v[column_name + '_keep']].shape[0]))
                print('Somme masquee : {}'.format(v[~v[column_name + '_keep']][column_name]['sum'].sum()))
                print('Nombre total de cellules : {}'.format(v.shape[0]))
        return

    def mask_values(self, dict_df: dict) -> dict:
        """ Performs masking of primary secret.

        Needs to be performed after safe aggregation and secondary secret check.
        Input parameters :
        - dictf_df : dictionnary where each key is the columns on which it was groupedby,
                    and the value is the corresponding aggregated dataframe, with primary secret check

        Output :
        - final_dict_df : similar dictionnary as input, but the primary secrets are masked in the dataframes
        """
        final_dict_df = dict_df.copy()

        for k, v in final_dict_df.items():
            masked_df = v.copy()
            for column_name in self.relevant_column:
                masked_df.loc[~masked_df[column_name + '_keep'], masked_df.columns.get_level_values(
                    0) == column_name] = None  # participation etat useless ?
                masked_df.drop([column_name + '_keep'], axis=1, inplace=True, level=0)
            final_dict_df[k] = masked_df
        return final_dict_df


class Version4SafeAggregation:

    def __init__(self, dataframe: pd.DataFrame, columns_to_check: list, list_aggregation: list, dominance, frequence, *args, **kwargs):
        self.dataframe = dataframe
        self.frequence = frequence
        self.dominance = dominance
        self.columns_to_check = columns_to_check
        self.group_by = list_aggregation
        self.measure_types = MEASURE_TYPES.union({max_percentage})
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
        safe_df = self.check_secret(aggregated_df)
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

    def check_secret(self, df: pd.DataFrame) -> pd.DataFrame:
        for col_secret in self.columns_to_check:
            df = self.check_max_percent(df, col_secret)
            df = self.check_count(df, col_secret)
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