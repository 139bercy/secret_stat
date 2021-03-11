# secret_stat
Secret Stat est un module python qui masque un jeu de donnée fournis en entrée.
Secret statistique, va créer des aggregations selon une liste de listes fournis en entrée puis va appliquer la règle de dominance et de fréquence qur les colonnes ciblées.

Si il y 3 ou moins de champs les informations de la ligne serons remplacé par des valeurs null. 

Si il y à une valeur qui reprèsente plus de 85% les champs dans la ligne serons remplacés par des valeurs null.

Les valeurs de dominance et fréquence peuvent être changés lors de l'appel de la fonction `apply_secret_stat(dataframe, columns_to_check, list_aggregation, dominance, frequence)`.

Le module, secret statistique prend un dataframe de la librairie Pandas en entrée et fournis un dictionnaire de dataframes en sortie. Pour l'utiliser il faut appeler la fonction suivante : `apply_secret_stat(dataframe, columns_to_check, list_aggregation)`

## Librairie requise :
 - Pandas

## input :
 - Pandas.DataFrame, dataframe
 - list, columns_to_check : columns on which we check the secret
 - list, list_aggregation : list of list aggregation

### input option :
 - int, dominance (default: 85)
 - int, frequence (default: 3)

## output :

 - dictionary of dataframe aggregated, dict keys are the items in the list of list aggregation

## Example :
`apply_secret_stat(...)`
 - `list_aggregation`, `list of list` les aggregation à réaliser
     - example:
        ```
       list_aggregation = [
            ["CODE_REGION", "CODE_DEPARTEMENT"],
            ["CODE_REGION", "TYPE_ENTREPRISE"]
       ]
       ``` 
 - `columns_to_check`, `list` columns on which we check secret
 - `dataframe`, `Pandas.DataFrame` le jeu de données
