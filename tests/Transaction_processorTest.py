import unittest
import os
import sys
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Scripts import Transaction_processor as TransactionProcessor

class TransactionProcessorTest(unittest.TestCase):
    """Unit tests for the TransactionProcessor class."""
 
    def setUp(self):
        """Creates test DataFrames before each test."""
        transaction_data = {
            "InvoiceNo": ["536365", "536366", "536367", "536368"],
            "InvoiceNo": ["85123A", "71053", "84406B", "84029G"],
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
        self.assertEqual(len(supplier_sales), 0)  # All suppliers should be accounted for
        
    def test_calcul_stat_data(self):
        """Test de l'analyse du produit le plus rentable en France et de l'heure la plus active."""
        processor = TransactionProcessor(self.df.copy(), self.supplier_df.copy())
        processor.calculate_total_amount()
        best_product, busiest_hour = processor.calcul_stat_data()
        self.assertEqual(best_product, "Product A")  # Produit A est le plus rentable en France
        self.assertEqual(busiest_hour, 8)  # Heure avec le plus de transactions
 
if __name__ == "__main__":
    unittest.main()