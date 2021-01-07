# secret_stat
`import secret_stat`

`apply_secret_stat(...)`
 - DATA_PATH, path to entry csv
 - GroupBy, list of list on how to group
     - example:
        ```
       GroupBy = [
            ("CODE_REGION", "CODE_DEPARTEMENT"),
            ("CODE_REGION", "TYPE_ENTREPRISE"),
            ("CODE_REGION", "MESURE"),
            ("CODE_REGION", "FILIÈRE"),
            ("MESURE", "FILIÈRE"),
            ("MESURE", "TYPE_ENTREPRISE"),
       ]
       ``` 
 - ImportantColumns, columns on which we will apply secrets
 - separator, the separator to read the csv, default : ` | `
