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