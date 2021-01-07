# secret_stat

## imput:
 - csv or dataframe
 - columns on which secret should be applied
 - column on which we check the secret
## output:

 - dataframe of dataframe aggregated
 - csv

`apply_secret_stat(...)`
 - `group_by`, list of list on how to group
     - example:
        ```
       group_by = [
            ("CODE_REGION", "CODE_DEPARTEMENT"),
            ("CODE_REGION", "TYPE_ENTREPRISE"),
            ("CODE_REGION", "MESURE"),
            ("CODE_REGION", "FILIÈRE"),
            ("MESURE", "FILIÈRE"),
            ("MESURE", "TYPE_ENTREPRISE"),
       ]
       ``` 
 - `columns_apply_secret` columns on which we will apply secrets, list
 - `column_to_check` column on which we check secret
 - `data_path` path to csv entry 
 - `separator` the separator to read the csv, default : ` | `
 - `dataframe` pandas.dataframe entry, only data_path or dataframe should be use at once
 - `export_to_csv` if "True" it will export the aggregation to csv, None by default
 - `path_to_export` path where you want the csv to be saved
