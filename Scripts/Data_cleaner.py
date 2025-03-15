import pandas as pd
import logging
import unittest
import warnings

warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(
    filename="logs/data_cleaner.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
 
class DataCleaner:
    """
    Class for Data cleaning
    It contains the methods :
    - remove_duplicates which removes duplicates
    - handle_missing_values which handles missing values
    - filter_valid_transactions which filters out canceled orders
    """
    def __init__(self, df: pd.DataFrame):
        """
        Initialize with the dataset.
        
        :param df: Pandas DataFrame containing the raw data.
        """
        self.df = df
        self.canceled_df = None  # Store canceled transactions separately
        logging.info("DataCleaner initialized with dataset of shape %s", self.df.shape)
 
    def remove_duplicates(self):
        """Remove duplicate rows from the dataset."""
        initial_shape = self.df.shape
        self.df = self.df.drop_duplicates()
        logging.info("Removed duplicates: %d rows removed. New shape: %s",
                     initial_shape[0] - self.df.shape[0], self.df.shape)
 
    def handle_missing_values(self):
        """
        Handle missing values in critical columns:
        - CustomerID: Drop rows with missing values.
        - Description: Fill missing product descriptions with 'Unknown'.
        """
        initial_shape = self.df.shape
        self.df.dropna(subset=["CustomerID", "Quantity"], inplace=True)
        self.df["Description"].fillna("Unknown", inplace=True)
        logging.info("Handled missing values: %d rows removed. New shape: %s",
                     initial_shape[0] - self.df.shape[0], self.df.shape)

 
    def filter_valid_transactions(self):
        """Séparer les transactions annulées au lieu de les supprimer."""
        self.canceled_df = self.df[self.df["InvoiceNo"].astype(str).str.startswith("C")].copy()
        self.df = self.df[~self.df["InvoiceNo"].astype(str).str.startswith("C")]
        logging.info("Separated canceled transactions. Valid transactions: %s, Canceled transactions: %s",
                     self.df.shape, self.canceled_df.shape)


    def get_cleaned_data(self):
        """Retourne les données nettoyées et les transactions annulées."""
        return self.df, self.canceled_df

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