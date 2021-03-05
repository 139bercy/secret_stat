import json

import numpy as np

import utils
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


class Version3SafeAggregation(SafeAgregation):
    """ Performs secondary secret check and removal.

    Attributes :
    - arguments inherited from base class
    - common_column : column that could be source for leaking secondary secrets
    - frequency_threshold : the threshold for the primary frequency rule
    - dominance_threshold : the threshold for the primary dominance rule


    Public methods available :
    - check_and_apply_secondary_secret : checks and masks secondary secret
    """

    def __init__(self, common_column: str, secret_columns: list, *args, **kwargs):
        super().__init__(columns_apply_secret=secret_columns, *args, **kwargs)
        self.common_column = common_column
        self.frequency_threshold = next(item for item in self.rules_list if item["RULE_NAME"] == "FREQUENCY")[
            'THRESHOLD']
        self.dominance_threshold = next(item for item in self.rules_list if item["RULE_NAME"] == "DOMINANCE")[
            'THRESHOLD']

    def _compute_columns_secondary_secret(self, df: pd.DataFrame, column_names) -> pd.DataFrame:
        aggregation_dict = {(col_lvl1, col_lvl2): func for col_lvl1 in self.relevant_column
                            for (col_lvl2, func) in LIST_FUNCTIONS}

        grouped_df = df.groupby(column_names,
                                as_index=False).agg(aggregation_dict)
        return grouped_df

    def _get_column_disclosure(self, df: pd.DataFrame, column_name: str) -> pd.DataFrame:
        # For each region, and each type of sum, checks if disclosed and if disclosable.
        # Input parameters :
        # Output : DataFrame with 3 columns : Region code, disclosable, disclosed

        disclosable = True
        disclosed = True

        if False in df[column_name + '_keep'].value_counts().index:
            secrets = df[df[column_name + '_keep'] == False][column_name]

            frequency_rule = (secrets['count'].sum() > self.frequency_threshold)
            dominance_rule = ((secrets['max'].max() * 100) < self.dominance_threshold * secrets['sum'].sum())
            disclosable = frequency_rule & dominance_rule
            disclosed = False

        return pd.DataFrame({self.common_column: df[self.common_column].values[0],
                             'disclosable_' + column_name: [disclosable],
                             'disclosed_' + column_name: [disclosed]})

    def _get_full_disclosion_df(self, init_df: pd.DataFrame) -> pd.DataFrame:
        final_disclosure = pd.DataFrame()

        for column_name in self.relevant_column:
            df_grouped = self._compute_columns_secondary_secret(init_df,
                                                                [self.common_column, column_name + '_keep'])

            column_disclosure = df_grouped.groupby(self.common_column, as_index=False).apply(
                func=lambda x: self._get_column_disclosure(x, column_name))

            if final_disclosure.empty:
                final_disclosure = column_disclosure.copy()
            else:
                final_disclosure = pd.merge(final_disclosure,
                                            column_disclosure,
                                            on=self.common_column,
                                            how='inner',
                                            validate='1:1')
        return final_disclosure

    def _mask_secondary_secret(self,
                               df_to_mask: pd.DataFrame,
                               df_1: pd.DataFrame,
                               df_2: pd.DataFrame,
                               column_name: str,
                               verbose: bool,
                               columns_apply_secret: list) -> pd.DataFrame:

        list_regions = df_2[[disclosed > disclosable for (disclosable, disclosed) in
                             zip(df_1['disclosable_' + column_name],
                                 df_2['disclosed_' + column_name])]][self.common_column].values

        masked_df = df_to_mask.copy()

        with open("config.json") as f:
            config = json.load(f)

        if len(list_regions) != 0:
            if verbose:
                print('{} cellules sur lesquelles le secret secondaire doit être apposé, regions : {}'.format(
                    len(list_regions),
                    list_regions))
            for region in list_regions:
                for col in columns_apply_secret:
                    index_min = (
                        masked_df[masked_df[self.common_column] == region][(col, 'sum')]).idxmin()
                    masked_df.loc[index_min, masked_df.columns.get_level_values(0) == column_name] = None
        else:
            if verbose:
                print('Aucune cellule sur lesquelles il faut apposer le secret secondaire')
        return masked_df

    def check_and_apply_secondary_secret(self,
                                         df_dict: dict,
                                         gb_keys: list,
                                         columns_apply_secret: list,
                                         verbose: bool = False) -> dict:
        """ Check and mask secondary secrets.

        Input parameters :
        - df_dict : dictionnary that is output of the perform_multiple_safe_aggregation from base class
        - gb_keys : list of the dictionnary's keys
        - verbose [optional] : default is False, gather insights on masked cells

        Output :
        final_dict_df : similar dictionnary as the input, with the secondary secrets masked.
        """
        final_dict_df = df_dict.copy()

        df_0 = final_dict_df[gb_keys[0]].copy()
        df_1 = final_dict_df[gb_keys[1]].copy()

        disclosure_df_0 = self._get_full_disclosion_df(df_0)
        disclosure_df_1 = self._get_full_disclosion_df(df_1)

        for column_name in self.relevant_column:
            df_1 = self._mask_secondary_secret(df_1, disclosure_df_0, disclosure_df_1, column_name, verbose,
                                               columns_apply_secret)
            df_0 = self._mask_secondary_secret(df_0, disclosure_df_1, disclosure_df_0, column_name, verbose,
                                               columns_apply_secret)

        final_dict_df[gb_keys[0]] = df_0
        final_dict_df[gb_keys[1]] = df_1

        return final_dict_df


class Version4SafeAggregation:

    def __init__(self, dataframe: pd.DataFrame, columns_to_check: list, columns_to_mask: list, *args, **kwargs):
        self.dataframe = dataframe
        self.columns_to_check = columns_to_check
        self.columns_to_mask = columns_to_mask
        self.group_by = columns_to_mask
        self.measure_types = MEASURE_TYPES.union({max_percentage})
        self.dict_aggreg = self._create_dict_aggregation(columns_to_check)
        with open("config.json") as f:
            config = json.load(f)
        self.rules_list = config["RULES"]

    def _create_dict_aggregation(self, list_targets: list) -> dict:
        final_dict = {target: self.measure_types for target in list_targets}
        return final_dict

    def aggregateFactory(self) -> dict:  # pour chaque clef créer un dataframe avec les données censurées
        dict_df = {}
        for gb_key in self.group_by:
            dict_df[gb_key] = (self.safe_aggregate(self.dataframe, gb_key))
        return dict_df

    def safe_aggregate(self, df: pd.DataFrame, gb_keys: list) -> pd.DataFrame:
        aggregated_df_3D = df.groupby(list(gb_keys)).agg(self.dict_aggreg).reset_index(level=0, drop=True)
        aggregated_df = self.dataframe_3D_to_2D(aggregated_df_3D)
        safe_df = self.check_secret(aggregated_df)
        return safe_df

    def dataframe_3D_to_2D(self, df3D: pd.DataFrame) -> pd.DataFrame:
        df3D.columns = ['_'.join(col) for col in df3D.columns.values]
        df2D = df3D.copy()
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
        df_percent.loc[df_percent[col_secret] >= 85] = np.nan
        return df_percent

    def check_count(self, df: pd.DataFrame, col_secret: str) -> pd.DataFrame:
        df_count = df.copy()
        name = "_count"
        col_secret = col_secret + name
        df_count.loc[df_count[col_secret] <= 3] = np.nan
        return df_count