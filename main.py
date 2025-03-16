import streamlit as st
import pandas as pd
import altair as alt
import json
import folium
from streamlit_folium import folium_static

# Load data
def load_data():
    file_path = "output/semi_cleaned_data.json"
    with open(file_path, "r") as file:
        return pd.DataFrame(json.load(file))

df = load_data()

# Ensure correct data types
df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce").fillna(0)

# Convert InvoiceDate to datetime
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], unit='ms')

# Remove negative quantities (returns)
df = df[df["Quantity"] > 0]

# Standardize Description (remove extra spaces, convert to uppercase)
df["Description"] = df["Description"].str.strip().str.upper()

# Calculate Total Sales correctly
df["TotalSales"] = df["Quantity"] * df["UnitPrice"]

# Format TotalSales for business readability
def format_sales(value):
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M €"  # Millions
    elif value >= 1_000:
        return f"{value / 1_000:.1f}K €"  # Thousands
    return f"{value:.2f} €"  # Default

st.title("Sales Dashboard")
# --- 1️⃣ Total Sales by Continent ---
sales_by_continent = df.groupby("Continent", as_index=False)["TotalSales"].sum()
sales_by_continent["FormattedSales"] = sales_by_continent["TotalSales"].apply(format_sales)

# Create Altair bar chart for continents
continent_chart = alt.Chart(sales_by_continent).mark_bar().encode(
    x=alt.X("TotalSales:Q", title="Total Sales (€)"),
    y=alt.Y("Continent:N", sort="-x", title="Continent"),
    tooltip=["Continent", "FormattedSales"]
).properties(title="Total Sales by Continent")

st.altair_chart(continent_chart, use_container_width=True)

# --- 2️⃣ Top 10 Products by Sales ---
sales_by_product = df.groupby(["StockCode", "Description"], as_index=False)["TotalSales"].sum()
sales_by_product = sales_by_product.sort_values(by="TotalSales", ascending=False).head(10)
sales_by_product["FormattedSales"] = sales_by_product["TotalSales"].apply(format_sales)

# Create Altair bar chart for top products
product_chart = alt.Chart(sales_by_product).mark_bar().encode(
    x=alt.X("TotalSales:Q", title="Total Sales (€)"),
    y=alt.Y("Description:N", sort="-x", title="Product"),
    tooltip=["Description", "FormattedSales"]
).properties(title="Top 10 Products by Sales")

st.altair_chart(product_chart, use_container_width=True)

# --- 3️⃣ Sales Over Time ---
sales_over_time = df.resample('M', on="InvoiceDate")["TotalSales"].sum().reset_index()
sales_over_time["FormattedSales"] = sales_over_time["TotalSales"].apply(format_sales)

# Create Altair line chart for sales over time
time_chart = alt.Chart(sales_over_time).mark_line().encode(
    x=alt.X("InvoiceDate:T", title="Date"),
    y=alt.Y("TotalSales:Q", title="Total Sales (€)"),
    tooltip=["InvoiceDate", "FormattedSales"]
).properties(title="Sales Over Time")

st.altair_chart(time_chart, use_container_width=True)

# --- 4️⃣ Continent-Level Map using Folium ---
continent_coords = {
    "Europe": [50, 10],
    "North America": [40, -100],
    "South America": [-15, -60],
    "Asia": [30, 100],
    "Africa": [0, 20],
    "Oceania": [-25, 135]
}

# Create Folium map
m = folium.Map(location=[20, 0], zoom_start=2)

# Add continent markers
for _, row in sales_by_continent.iterrows():
    folium.CircleMarker(
        location=continent_coords.get(row["Continent"], [0, 0]),
        radius=max(5, row["TotalSales"] / 1_000_000),  # Scale circle size
        color="blue",
        fill=True,
        fill_color="blue",
        fill_opacity=0.6,
        popup=f"{row['Continent']}: {row['FormattedSales']}"
    ).add_to(m)
    
# Display Folium map
st.write("🌍 **Sales by Continent (Map) **")
folium_static(m)

