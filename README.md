# secret_stat
DO NOT MERGE THIS BRANCHE
## imput:
 - csv or dataframe
 - columns on which secret should be applied
 - column on which we check the secret
## output:

 - dictionary of dataframe aggregated, dict keys are the group_by list
 - csv

`apply_secret_stat(...)`
 - `group_by`, `list of list` on how to group
     - example:
        ```
       group_by = [
            ("CODE_REGION", "CODE_DEPARTEMENT"),
            ("CODE_REGION", "TYPE_ENTREPRISE")
       ]
       ``` 
 - `columns_apply_secret`, `list` columns on which we will apply secrets, list
 - `column_to_check`, `str` column on which we check secret
 - `data_path`, `str` path to csv entry `
 - `separator`, `str` the separator to read the csv, default : ` | `
 - `dataframe`, `pandas.dataframe` entry, only data_path or dataframe should be use at once
 - `export_to_csv`, `bool` if "True" it will export the aggregation to csv, None by default
 - `path_to_export`, `str` path where you want the csv to be saved
