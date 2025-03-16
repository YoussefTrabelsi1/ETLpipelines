# Online Retail ETL Pipeline

## 📌 Project Overview
This project is an **ETL (Extract, Transform, Load) pipeline** designed to process an **online retail dataset**. The pipeline cleans, transforms, and aggregates sales transaction data to provide **business insights**, including:
- Ranking **continents by total spending**.
- Identifying **the continent with the most canceled transactions**.
- Analyzing **sales trends by country and supplier**.
- Finding the **busiest transaction hours** and **best-selling products**.

The project is implemented using **Python** with a structured **Object-Oriented Programming (OOP)** approach.

---

## 💒 Project Structure

```
📆 Online-Retail-ETL
👉👉 data/                     # Data folder (raw datasets)
│    ├── Online Retail.xlsx      # Transaction dataset
│    ├── Supplier.csv            # Supplier information
│    ├── continent_mapping_full.csv # Country-to-continent mapping
│
👉👉 logs/                     # Logs for debugging
│    ├── etl_pipeline.log
│    ├── data_cleaner.log
│    ├── transaction_processor.log
│
👉👉 output/                    # Processed results
│    ├── processed_data.parquet   # Final cleaned dataset
│
👉👉 tests/                     # Unit tests for all classes
│    ├── DataCleanerTest.py
│    ├── TransactionProcessorTest.py
│
│── ETLPipeline.py               # Main ETL pipeline script
│── Data_cleaner.py               # Handles data cleaning
│── Transaction_processor.py      # Handles data transformations & analysis
│── README.md                     # Project documentation
│── requirements.txt               # Dependencies
│── .gitignore                     # Ignore unnecessary files
```

---

## 🚀 Features

### 🧹 Data Cleaning (`Data_cleaner.py`)
- **Removes duplicate rows**.
- **Handles missing values** in key columns.
- **Separates canceled transactions** for further analysis instead of deleting them.

### 📊 Transaction Processing (`Transaction_processor.py`)
- **Calculates total sales per transaction**.
- **Aggregates monthly and country-wise sales**.
- **Finds the best-selling product in France**.
- **Determines the busiest transaction hour**.
- **Ranks suppliers based on sales**.
- **Classifies continents by spending**.
- **Identifies the continent with the most canceled transactions**.

### 🔄 ETL Orchestration (`ETLPipeline.py`)
- **Executes the full ETL pipeline**.
- **Loads, cleans, and transforms data**.
- **Exports final processed data to Parquet format**.

---

## 📝 Installation & Usage

### ⚡ Prerequisites
- Python 3.9+
- Required Python packages listed in `requirements.txt`.

### ⚙ Setup
1. Clone this repository:
   ```bash
   git clone https://github.com/YoussefTrabelsi1/ETLpipelines.git
   cd Online-Retail-ETL
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### 🚀 Run the ETL Pipeline
```bash
python ETLPipeline.py
```

This will process the dataset and save the cleaned results in `output/processed_data.parquet`.

---

## 🔧 Testing
Run unit tests using:
```bash
python -m unittest discover tests/
```

---

## 📚 License
This project is licensed under the MIT License.

---

## 👥 Contributors
- **Youssef Trabelsi** - Initial development

Feel free to contribute by creating pull requests or reporting issues!

---

## 🔗 Contact
For any inquiries, contact me at `yousseftrabelsi250@gmail.com`.