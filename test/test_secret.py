import json
import os
import pytest
import pandas as pd
from pandas._testing import assert_frame_equal
from scripts_python.PlanRProcesses.ExcelProcessBPI import ExcelProcessBPI
from scripts_python.PlanRProcesses.ExcelProcess import ExcelProcessRELANCE

@pytest.fixture
def epr():
    return ExcelProcessBPI(ExcelProcessRELANCE)

@pytest.fixture
def get_conf():
    get_conf = "conf/"
    return get_conf

@pytest.fixture
def path_to_data(get_conf):
    return "./scripts_python/test/fake_data/"

@pytest.fixture
def params(get_conf):
    with open(os.path.join(get_conf, "parameters.json")) as f:
        params = json.load(f)
    return params

@pytest.fixture
def stock_etablissement(path_to_data, params):
    stock_etablissement = os.path.join(path_to_data, params["ref_StockEtablissement"])
    return stock_etablissement


def test_format_dataframe(epr):
    # test pour vérifier que les accents et les colonnes sont bient traité
    test = {"Projet": ["nom du projet"],
            "ENtREPrISE": ["MINISTERE DE L'ECONOMIE DES FINANCES ET DE LA RELANCE"],
            "TYPE eNTREPRISE": ["PME"],
            "SIreN": [110020013],
            "SIREt": [11002001300097],
            "Département": [75012],
            "Ville": ["Paris"],
            "MonTANT INVESTISSEMENT": [10000],
            "DESCRIPTION pROJET": ["belle description de ce projet"],
            "RETOMBEES PROJET": ["belle retombées du projet"],
            "DATE DEPOT PROJET": ["2016/05/12"],
            "DATE DEBUT INSTRUCTION": ["2016/05/12"],
            "MONTANT PARTICIPATION éTAT": [5000],
            "DATE DECISION": ["2016/05/12"],
            "Statut": ["decidé"]
            }

    expected = {"PROJET": ["nom du projet"],
                "ENTREPRISE": ["MINISTERE DE L'ECONOMIE DES FINANCES ET DE LA RELANCE"],
                "TYPE_ENTREPRISE": ["PME"],
                "SIREN": [110020013],
                "SIRET": [11002001300097],
                "DEPARTEMENT": [75012],
                "VILLE": ["Paris"],
                "MONTANT_INVESTISSEMENT": [10000],
                "DESCRIPTION_PROJET": ["belle description de ce projet"],
                "RETOMBEES_PROJET": ["belle retombées du projet"],
                "DATE_DEPOT_PROJET": ["2016/05/12"],
                "DATE_DEBUT_INSTRUCTION": ["2016/05/12"],
                "MONTANT_PARTICIPATION_ETAT": [5000],
                "DATE_DECISION": ["2016/05/12"],
                "STATUT": ["decidé"]
                }
    expected_df = pd.DataFrame(expected)
    epr.df = pd.DataFrame(test)
    epr.format_dataframe()
    assert_frame_equal(expected_df, epr.df, check_names=True)
