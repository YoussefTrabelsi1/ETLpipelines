import unittest
import os
import sys
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Scripts import Data_cleaner as DataCleaner

class DataCleanerTest(unittest.TestCase):
    """Test unitaire pour la classe DataCleaner"""
 
    def setUp(self):
        """Créer un DataFrame de test avant chaque test."""
        data = {
            "InvoiceNo": ["536365", "536366", "C536367", "536368", "536368"],
            "StockCode": ["85123A", "71053", "84406B", "84029G", "84029G"],
            "Description": ["WHITE HANGING HEART T-LIGHT HOLDER", "ASSORTED COLOUR BIRD ORNAMENT", None, "KNITTED UNION FLAG HOT WATER BOTTLE", "KNITTED UNION FLAG HOT WATER BOTTLE"],
            "Quantity": [6, 6, -2, 8, 8],
            "InvoiceDate": ["2010-12-01 08:26", "2010-12-01 08:28", "2010-12-01 08:34", "2010-12-01 08:34", "2010-12-01 08:34"],
            "UnitPrice": [2.55, 3.39, 5.00, 3.39, 3.39],
            "CustomerID": [17850, 17850, None, 13047, 13047],
            "Country": ["United Kingdom", "United Kingdom", "United Kingdom", "France", "France"]
        }
        self.df = pd.DataFrame(data)
 
    def test_remove_duplicates(self):
        """Tester la suppression des doublons"""
        cleaner = DataCleaner(self.df.copy())
        cleaner.remove_duplicates()
        self.assertEqual(len(cleaner.df), 4)  # Il y avait 5 lignes avec une duplication
 
    def test_handle_missing_values(self):
        """Tester la gestion des valeurs manquantes"""
        cleaner = DataCleaner(self.df.copy())
        cleaner.handle_missing_values()
        self.assertFalse(cleaner.df["CustomerID"].isnull().any())  # Aucun CustomerID ne doit être NaN
        self.assertFalse(None in cleaner.df["Description"].values)  # Les descriptions vides doivent être remplacées
 
    def test_filter_valid_transactions(self):
        """Tester la suppression des transactions annulées"""
        cleaner = DataCleaner(self.df.copy())
        cleaner.filter_valid_transactions()
        self.assertFalse(any(cleaner.df["InvoiceNo"].astype(str).str.startswith("C")))  # Aucune transaction annulée
 
    def test_full_cleaning_pipeline(self):
        """Tester le pipeline complet de nettoyage"""
        cleaner = DataCleaner(self.df.copy())
        cleaner.remove_duplicates()
        cleaner.handle_missing_values()
        cleaner.filter_valid_transactions()
        
        # Vérifier si tout est bien nettoyé
        self.assertEqual(len(cleaner.df), 3)  # Après nettoyage, 3 transactions valides
        self.assertFalse(cleaner.df["CustomerID"].isnull().any())  # Aucun CustomerID ne doit être NaN
        self.assertFalse(None in cleaner.df["Description"].values)  # Description remplacée
        self.assertFalse(any(cleaner.df["InvoiceNo"].astype(str).str.startswith("C")))  # Pas de transactions annulées

if __name__=="__main__":
    unittest.main()