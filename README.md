# secret_stat
Secret Stat est un module python qui masque un jeu de donnée fournis en entrée.

Secret statistique, va créer des agrégations, selon une liste de listes fournies en entrée puis va appliquer la règle de dominance et de fréquence sur les colonnes ciblées.

S'il y a 3 ou moins de champs les informations de la ligne serons remplacées par des valeurs null.

S'il y a une valeur qui reprèsente plus de 85% les champs dans la ligne serons remplacés par des valeurs null.

Les valeurs de dominance et fréquence peuvent être changés lors de l'appel de la fonction `apply_secret_stat(dataframe, columns_to_check, list_aggregation, dominance, frequence)`.

Le module, secret statistique prend un dataframe de la librairie Pandas en entrée et fournis un dictionnaire de dataframes en sortie. Pour l'utiliser il faut appeler la fonction suivante : `apply_secret_stat(dataframe, columns_to_check, list_aggregation)`
## Librairie requise :
 - Pandas

## input :
 - Pandas.DataFrame, dataframe
 - list, columns_to_check : colonnes sur lesquelles on regarde pour appliquer les secrets
 - list, list_aggregation : liste of listes d'agrégations

### input option :
 - int, dominance (default: 85)
 - int, frequence (default: 3)

## output :

 - dictionary of dataframe aggregated, dict keys are the items in the list of list aggregation
 - Dictionnaire de Pandas.DataFrame agrégés, les clefs du dictionnaire sont les listes contenue dans la liste `list_aggregation`
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
 - `columns_to_check`, `list` colonnes sur lesquelles on regarde pour appliquer les secrets
 - `dataframe`, `Pandas.DataFrame` le jeu de données
