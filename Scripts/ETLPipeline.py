import pandas as pd
import logging
from Data_cleaner import DataCleaner
from Transaction_processor import TransactionProcessor
 
# Configure logging
logging.basicConfig(
    filename="logs/etl_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
 
class ETLPipeline:
    """Orchestrates the entire ETL process, including data cleaning, transformation, and storage."""
 
    def __init__(self, retail_data_path: str, supplier_data_path: str, continent_mapping: str):
        """
        Initializes the ETL pipeline by loading the datasets.
        
        :param retail_data_path: Path to the Online Retail Excel file.
        :param supplier_data_path: Path to the Supplier CSV file.
        """
        try:
            self.raw_df = pd.read_excel(retail_data_path)  # Load raw data before cleaning
            self.raw_df.drop_duplicates(inplace=True)  # Remove duplicate transactions
            self.df = self.raw_df.copy()
            
            self.supplier_df = pd.read_csv(supplier_data_path)
            self.continent_mapping = pd.read_csv(continent_mapping)


            logging.info("Datasets loaded successfully. Retail data shape: %s, Supplier data shape: %s",
                         self.df.shape, self.supplier_df.shape)
        except Exception as e:
            logging.error("Error loading datasets: %s", str(e))
            raise RuntimeError("Failed to load datasets.") from e
 
    def run_pipeline(self):
        """Executes the full ETL process: data cleaning, transformations, and processing."""
        try:
            logging.info("Starting ETL pipeline...")


            # Step 1: Data Cleaning
            cleaner = DataCleaner(self.df)
            cleaner.remove_duplicates()
            cleaner.handle_missing_values()
            cleaner.filter_valid_transactions()
            self.df, self.canceled_df = cleaner.get_cleaned_data()  # Get cleaned and canceled data

            logging.info("Data cleaning completed. Cleaned data shape: %s, Canceled transactions shape: %s",
                        self.df.shape, self.canceled_df.shape)

            # Step 2: Transaction Processing
            processor = TransactionProcessor(self.df, self.canceled_df, self.supplier_df, self.continent_mapping)
            processor.calculate_total_amount()
            country_sales = processor.group_by_country()
            monthly_stats = processor.aggregate_monthly_data()
            best_product, busiest_hour = processor.calcul_stat_data()
            supplier_sales, uk_2011_sales = processor.aggregate_supplier_data()
            continent_sales, continent_with_most_cancellations = processor.aggregate_world_data()

            logging.info("Transaction processing completed.")

            # Final Data Storage
            self.df["TotalAmount"] = self.df["Quantity"] * self.df["UnitPrice"]
            final_results = {
                "cleaned_data": self.df,
                "country_sales": country_sales,
                "monthly_stats": monthly_stats,
                "best_product_in_france": best_product,
                "busiest_transaction_hour": busiest_hour,
                "supplier_sales": supplier_sales,
                "uk_2011_supplier_sales": uk_2011_sales,
                "continent_sales": continent_sales,
                "continent_with_most_cancellations": continent_with_most_cancellations
            }
            logging.info("ETL pipeline execution completed successfully.")
            return final_results

        except Exception as e:
            logging.error("ETL pipeline execution failed: %s", str(e))
            raise RuntimeError("ETL process failed.") from e
 
    def save_as_parquet(self, path: str):
        """Saves the cleaned and processed data to a Parquet file."""
        try:
            # Convert object columns to strings
            for col in self.df.select_dtypes(include=["object"]).columns:
                self.df[col] = self.df[col].astype(str)
    
            # Convert 'YearMonth' (Period type) to string, since fastparquet does not support it
            if "YearMonth" in self.df.columns:
                self.df["YearMonth"] = self.df["YearMonth"].astype(str)
    
            # Ensure numeric columns are properly formatted
            self.df = self.df.convert_dtypes()

            # Save to Parquet with 'fastparquet' engine
            self.df.to_parquet(path, index=False, engine="fastparquet")
            logging.info("Final processed data saved to Parquet: %s", path)
    
        except Exception as e:
            logging.error("Error saving to Parquet: %s", str(e))
            raise RuntimeError("Failed to save Parquet file.") from e
        
    def save_semi_cleaned_json(self, path: str):
        """Saves a semi-cleaned dataset (without duplicates and merged with continents/suppliers) to a JSON file."""
        try:
            # Step 1: Remove duplicates
            semi_cleaned_df = self.raw_df.drop_duplicates().copy()

            # Step 2: Merge with continent mapping
            semi_cleaned_df = semi_cleaned_df.merge(self.continent_mapping, on="Country", how="left")

            # Step 3: Merge with supplier data
            semi_cleaned_df = semi_cleaned_df.merge(self.supplier_df, on="InvoiceNo", how="left")

            # Step 4: Convert DataFrame to JSON
            semi_cleaned_df.to_json(path, orient="records", indent=4)

            logging.info("Semi-cleaned data saved to JSON: %s", path)

        except Exception as e:
            logging.error("Error saving semi-cleaned data to JSON: %s", str(e))
            raise RuntimeError("Failed to save semi-cleaned dataset.") from e


if __name__ == "__main__":
    
    etl = ETLPipeline("data/Online Retail.xlsx", "data/Supplier.csv","data/continent_mapping_full.csv")  # Paths to your datasets
    #etl = ETLPipeline("data\onlie retail test.xlsx", "data/Supplier.csv","data/continent_mapping_full.csv")  # Paths to your datasets
    results = etl.run_pipeline()
    etl.save_as_parquet("output/processed_data.parquet")
    # Save semi-cleaned dataset to JSON
    etl.save_semi_cleaned_json("output/semi_cleaned_data.json")

 
    # Display key results
    print("Best-selling product in France:", results["best_product_in_france"])
    print("Busiest transaction hour:", results["busiest_transaction_hour"])