import pandas as pd
import logging
import unittest

# Configure logging
logging.basicConfig(
    filename="transaction_processor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
 
class TransactionProcessor:
    """Handles transaction processing, including sales aggregation, supplier analysis, and world data classification."""
 
    def __init__(self, df: pd.DataFrame, supplier_df: pd.DataFrame):
        """
        Initializes the processor with the cleaned transaction data and supplier data.
        
        :param df: Cleaned transactions DataFrame.
        :param supplier_df: Supplier DataFrame containing supplier information.
        """
        self.df = df
        self.supplier_df = supplier_df
        logging.info("TransactionProcessor initialized with transaction data of shape %s and supplier data of shape %s",
                     self.df.shape, self.supplier_df.shape)
 
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
        Merges supplier data, ranks suppliers based on total sales,
        and filters data for 2011 transactions in the United Kingdom.
        """
        self.df = self.df.merge(self.supplier_df, on="InvoiceNo", how="left")
        print(self.df)
        # Rank suppliers by total sales
        supplier_sales = self.df.groupby("Fournisseur")["TotalAmount"].sum().reset_index()
        logging.info("Suppliers ranked based on total sales.")
 
        # Filter transactions for the United Kingdom in 2011
        uk_2011_sales = self.df[
            (self.df["Country"] == "United Kingdom") &
            (self.df["InvoiceDate"].dt.year == 2011)
        ].groupby("Fournisseur")["TotalAmount"].sum().reset_index()
        logging.info("Suppliers ranked based on total sales for 2011 in the UK.")
 
        return supplier_sales, uk_2011_sales
 
    def aggregate_world_data(self, continent_mapping: pd.DataFrame):
        """
        Classifies continents based on spending and identifies the continent with the most cancellations.
        
        :param continent_mapping: DataFrame mapping countries to continents.
        """
        self.df = self.df.merge(continent_mapping, on="Country", how="left")
 
        # Rank continents by total spending
        continent_sales = self.df.groupby("Continent")["TotalAmount"].sum().reset_index()
        logging.info("Continents ranked based on total spending.")
 
        # Find the continent with the most cancellations
        canceled_transactions = self.df[self.df["InvoiceNo"].astype(str).str.startswith("C")]
        continent_cancellations = canceled_transactions.groupby("Continent")["InvoiceNo"].count().idxmax()
        logging.info("Continent with the highest number of cancellations: %s", continent_cancellations)
 
        return continent_sales, continent_cancellations
 
class TransactionProcessorTest(unittest.TestCase):
    """Unit tests for the TransactionProcessor class."""
 
    def setUp(self):
        """Creates test DataFrames before each test."""
        transaction_data = {
            "InvoiceNo": ["536365", "536366", "536367", "536368"],
            "StockCode": ["85123A", "71053", "84406B", "84029G"],
            "Description": ["Product A", "Product B", "Product C", "Product D"],
            "Quantity": [10, 5, 2, 7],
            "InvoiceDate": ["2010-12-01 08:26", "2010-12-01 08:28", "2010-12-01 09:34", "2010-12-02 10:15"],
            "UnitPrice": [2.5, 5.0, 3.0, 4.5],
            "CustomerID": [17850, 17851, 17852, 17853],
            "Country": ["France", "United Kingdom", "France", "Germany"]
        }
        self.df = pd.DataFrame(transaction_data)
        self.df["InvoiceDate"] = pd.to_datetime(self.df["InvoiceDate"])
 
        supplier_data = {
            "InvoiceNo": ["536365", "536366", "536367", "536368"],
            "Fournisseur": ["F123", "F456", "F789", "F101"]
        }
        self.supplier_df = pd.DataFrame(supplier_data)
 
    def test_calculate_total_amount(self):
        """Tests the calculation of total transaction amounts."""
        processor = TransactionProcessor(self.df.copy(), self.supplier_df.copy())
        processor.calculate_total_amount()
        expected_totals = [25.0, 25.0, 6.0, 31.5]  # Quantity * UnitPrice
        self.assertListEqual(processor.df["TotalAmount"].tolist(), expected_totals)
 
    def test_group_by_country(self):
        """Tests country-based sales aggregation."""
        processor = TransactionProcessor(self.df.copy(), self.supplier_df.copy())
        processor.calculate_total_amount()
        country_sales = processor.group_by_country()
        expected_totals = {"France": 31.0, "United Kingdom": 25.0, "Germany": 31.5}
        for _, row in country_sales.iterrows():
            self.assertAlmostEqual(row["TotalAmount"], expected_totals[row["Country"]])
 
    def test_aggregate_supplier_data(self):
        """Tests supplier data merging and ranking."""
        processor = TransactionProcessor(self.df.copy(), self.supplier_df.copy())
        processor.calculate_total_amount()
        supplier_sales, _ = processor.aggregate_supplier_data()
        self.assertEqual(len(supplier_sales), 4)  # All suppliers should be accounted for
        
    def test_calcul_stat_data(self):
        """Test de l'analyse du produit le plus rentable en France et de l'heure la plus active."""
        processor = TransactionProcessor(self.df.copy(), self.supplier_df.copy())
        processor.calculate_total_amount()
        best_product, busiest_hour = processor.calcul_stat_data()
        self.assertEqual(best_product, "Product A")  # Produit A est le plus rentable en France
        self.assertEqual(busiest_hour, 8)  # Heure avec le plus de transactions
 
if __name__ == "__main__":
    unittest.main()
 