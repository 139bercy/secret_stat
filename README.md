# secret_stat

## input:
 - dataframe
 - list of list aggregation
 - column on which we check the secret
### option:
 - dominance, int (default: 85)
 - frequence, int (default: 3)

## output:

 - dictionary of dataframe aggregated, dict keys are the items in the list of list aggregation

`apply_secret_stat(...)`
 - `list_aggregation`, `list of list` on how to group
     - example:
        ```
       list_aggregation = [
            ("CODE_REGION", "CODE_DEPARTEMENT"),
            ("CODE_REGION", "TYPE_ENTREPRISE")
       ]
       ``` 
 - `columns_to_check`, `list` column on which we check secret
 - `dataframe`, `pandas.dataframe` entry, only data_path or dataframe should be use at once
