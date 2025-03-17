import pandas as pd
import logging
import unittest
import numpy as np

# Configure logging
logging.basicConfig(
    filename="logs/transaction_processor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
 
class TransactionProcessor:
    """Handles transaction processing, including sales aggregation, supplier analysis, and world data classification."""
 
    def __init__(self, df: pd.DataFrame, canceled_df: pd.DataFrame, supplier_df: pd.DataFrame, continent_mapping: pd.DataFrame):
        """
        Initializes the processor with the cleaned transaction data and supplier data.
        
        :param df: Cleaned transactions DataFrame.
        :param supplier_df: Supplier DataFrame containing supplier information.
        """
        self.df = df
        self.canceled_df = canceled_df  # Stocker les transactions annulées
        self.supplier_df = supplier_df
        self.continent_mapping = continent_mapping
        logging.info("TransactionProcessor initialized with data shape %s, and canceled transactions shape %s",
                     self.df.shape, self.canceled_df.shape)
    def calculate_total_amount(self):
        """Adds a TotalAmount column (Quantity * UnitPrice) for each transaction."""
        self.df["TotalAmount"] = self.df["Quantity"] * self.df["UnitPrice"]
        logging.info("TotalAmount column added successfully.")
 
    def group_by_country(self):
        """Groups transactions by country and calculates total sales."""
        country_sales = self.df.groupby("Country")["TotalAmount"].sum().reset_index()
        logging.info("Transactions grouped by country successfully.")
        return country_sales
 
    def aggregate_monthly_data(self):
        """Calculates monthly sales statistics, including total revenue and transaction count."""
        self.df["InvoiceDate"] = pd.to_datetime(self.df["InvoiceDate"])
        self.df["YearMonth"] = self.df["InvoiceDate"].dt.to_period("M")
        monthly_stats = self.df.groupby("YearMonth").agg(
            TotalSales=("TotalAmount", "sum"),
            TransactionCount=("InvoiceNo", "count")
        ).reset_index()
        logging.info("Monthly statistics calculated successfully.")
        return monthly_stats
 
    def calcul_stat_data(self):
        """Finds the most profitable product in France and identifies the busiest transaction hour."""
        france_df = self.df[self.df["Country"] == "France"]
        best_product = france_df.groupby("Description")["TotalAmount"].sum().idxmax()
        logging.info("Most profitable product in France: %s", best_product)
 
        self.df["Hour"] = self.df["InvoiceDate"].dt.hour
        busiest_hour = self.df.groupby("Hour")["InvoiceNo"].count().idxmax()
        logging.info("Busiest transaction hour: %d h", busiest_hour)
 
        return best_product, busiest_hour
 
    def aggregate_supplier_data(self):
        """
        Aggregates supplier sales data, ranking them based on total sales.
        Also, filters data for 2011 in the United Kingdom for separate analysis.
        :return: Tuple of DataFrames (global ranking, UK 2011 ranking)
        """
        # Ensure Quantity and UnitPrice are numeric
        self.df['Quantity'] = pd.to_numeric(self.df['Quantity'], errors='coerce')
        self.df['UnitPrice'] = pd.to_numeric(self.df['UnitPrice'], errors='coerce')
        
        # Remove canceled transactions
        df_valid = self.df[~self.df['InvoiceNo'].astype(str).str.startswith('C')]
        
        # Compute total sales per transaction
        df_valid['TotalAmount'] = df_valid['Quantity'] * df_valid['UnitPrice']
        
        # Normalize InvoiceNo type and format
        df_valid['InvoiceNo'] = df_valid['InvoiceNo'].astype(str).str.strip()
        self.supplier_df['InvoiceNo'] = self.supplier_df['InvoiceNo'].astype(str).str.strip()
        
        # Merge with supplier data using InvoiceNo
        df_merged = df_valid.merge(self.supplier_df, on='InvoiceNo', how='left')
        

        
        # Aggregate total sales per supplier
        df_supplier_sales = df_merged.groupby('Fournisseur')['TotalAmount'].sum().reset_index()
        
        # Rank suppliers based on total sales
        df_supplier_sales = df_supplier_sales.sort_values(by='TotalAmount', ascending=False)
        
        # Filter transactions for UK in 2011
        df_uk_2011 = df_valid[(df_valid['InvoiceDate'] >= '2011-01-01') & 
                              (df_valid['InvoiceDate'] < '2012-01-01') &
                              (df_valid['Country'] == 'United Kingdom')]
        
        # Compute total sales for UK 2011
        df_uk_2011_merged = df_uk_2011.merge(self.supplier_df, on='InvoiceNo', how='left')
        df_uk_2011_sales = df_uk_2011_merged.groupby('Fournisseur')['TotalAmount'].sum().reset_index()
        df_uk_2011_sales = df_uk_2011_sales.sort_values(by='TotalAmount', ascending=False)
        
        logging.info("Supplier aggregation completed. Returning results.")
        
        return df_supplier_sales, df_uk_2011_sales


    def aggregate_world_data(self):
        """
        Classe les continents selon les dépenses et identifie celui avec le plus d'opérations annulées.
        """
        self.df = self.df.merge(self.continent_mapping, on="Country", how="left")
        self.canceled_df = self.canceled_df.merge(self.continent_mapping, on="Country", how="left")

        # Calculer les dépenses par continent
        continent_sales = self.df.groupby("Continent")["TotalAmount"].sum().reset_index()
        logging.info("Continents ranked based on total spending.")

        # Trouver le continent avec le plus d'annulations
        continent_cancellations = self.canceled_df.groupby("Continent")["InvoiceNo"].count().idxmax()
        logging.info("Continent with the highest number of cancellations: %s", continent_cancellations)

        return continent_sales, continent_cancellations
 