# %%
import sklearn
import platform
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
import time
import datetime

# %%
# define path to data raw data
base_path_0 = "C:/Users/dsosa/Documents/snowflake_visual/data/raw"  # change here if another user is running the code

# %%
# load data from path
comparables_ytm = pd.read_csv(
    os.path.join(base_path_0, "comparables_ytm_hist.csv"),
    delimiter=",",
    header=0,
    low_memory=False,
)
#print("comparables_ytm shape ----->", comparables_ytm.shape)

comparables_names = pd.read_csv(
    os.path.join(base_path_0, "comparables_names.csv"),
    delimiter=",",
    header=0,
    low_memory=False,
)
#print("comparables_names shape ----->", comparables_names.shape)

panama_globales = pd.read_csv(
    os.path.join(base_path_0, "panama_globales_hist.csv"),
    delimiter=",",
    header=0,
    low_memory=False,
)
#print("panama_globales_to_spead shape ----->", panama_globales.shape)

# %% [markdown]
# # Panama Glob Transformations

# %%
panama_globales.head(2)

# %%
# replace 'Corp' in panama_globales dataset in any place where they are
panama_globales.columns = panama_globales.columns.str.replace("Corp", "").copy()

# %%
# Extract ISSUE_DT and MATURITY from the first two rows
issue_dt = panama_globales.iloc[0, 1:].values
maturity = panama_globales.iloc[1, 1:].values

# %%
# Create a new DataFrame for the main data starting from the third row
main_data = panama_globales.iloc[2:].reset_index(drop=True)

# %%
# Set the column names for the main data
main_data.columns = ["Dates"] + list(main_data.columns[1:])

# %%
# Melt the DataFrame to get 'Isin' and 'ytm'
melted_data = main_data.melt(id_vars=["Dates"], var_name="Isin", value_name="ytm")

# %%
# Create a dictionary to map Isin to ISSUE_DT and MATURITY
isin_map = {
    isin: {"ISSUE_DT": issue, "MATURITY": mat}
    for isin, issue, mat in zip(main_data.columns[1:], issue_dt, maturity)
}

# Add ISSUE_DT and MATURITY columns
melted_data["ISSUE_DT"] = melted_data["Isin"].map(lambda x: isin_map[x]["ISSUE_DT"])
melted_data["MATURITY"] = melted_data["Isin"].map(lambda x: isin_map[x]["MATURITY"])

# %%
# Convert 'Dates' to datetime
melted_data["Dates"] = pd.to_datetime(melted_data["Dates"])

# Convert 'ytm' to float, replacing empty strings with NaN
melted_data["ytm"] = pd.to_numeric(melted_data["ytm"], errors="coerce")

# Convert 'ISSUE_DT' and 'MATURITY' to datetime
melted_data["ISSUE_DT"] = pd.to_datetime(melted_data["ISSUE_DT"], errors="coerce")
melted_data["MATURITY"] = pd.to_datetime(melted_data["MATURITY"], errors="coerce")

# Sort the DataFrame
melted_data = melted_data.sort_values(["Dates", "Isin"]).reset_index(drop=True)

# %%
# rename melted_data and rename 'Dates' by 'date'
melted_data.rename(columns={"Dates": "date"}, inplace=True)

# %%
# define panama_blobales_ytm
panama_ytm_globales = melted_data.copy()

# %% [markdown]
# # Comparables 'YTM' Transformations

# %%
# drop spaces in columns names and drop 'corp' word
comparables_ytm.columns = comparables_ytm.columns.str.replace(" ", "").copy()
comparables_ytm.columns = comparables_ytm.columns.str.replace("corp", "").copy()
# rename all columns names
comparables_names.columns = comparables_names.columns.str.lower().str.replace(" ", "_")

# %%
# define date format
comparables_ytm["date"] = pd.to_datetime(
    comparables_ytm["date"], format="%m/%d/%Y"
).dt.strftime("%Y-%m-%d")

# transform date columns
comparables_names["issue_date"] = pd.to_datetime(
    comparables_names["issue_date"], format="%m/%d/%Y"
).dt.strftime("%Y-%m-%d")
comparables_names["maturity_2"] = pd.to_datetime(
    comparables_names["maturity_2"], format="%m/%d/%Y"
).dt.strftime("%Y-%m-%d")

# %% [markdown]
# # Create time to maturity Panama Glob

# %%
# Definir la fecha actual
from datetime import datetime

fecha_actual = datetime(2024, 12, 18)

# Calcular el tiempo hasta el vencimiento en años
# The 'MATURITY' column already contains Timestamp objects, so no need for strptime
panama_ytm_globales["time_to_maturity"] = panama_ytm_globales["MATURITY"].apply(
    lambda x: (x - fecha_actual).days / 365
)

# %%
# round time_to_maturity in case of
panama_ytm_globales["time_to_maturity_rounded"] = panama_ytm_globales[
    "time_to_maturity"
].round()

# %% [markdown]
# # Start Treasury Homologations


# %%
# bond_category equal to tresury
def categorize_bond(years):
    if years <= 0:
        return 0  # Matured bonds
    elif years <= 1:
        return 1  # 1-year bonds
    elif years <= 2:
        return 2  # 2-year bonds
    elif years <= 3:
        return 3  # 3-year bonds
    elif years <= 5:
        return 5  # 5-year bonds
    elif years <= 7:
        return 7  # 7-year bonds
    elif years <= 10:
        return 10  # 10-year bonds
    elif years <= 20:
        return 20  # 20-year bonds
    elif years <= 30:
        return 30  # 20-year bonds
    else:
        return 50  # Bonds longer than 30 years


# Apply the function to create a new column
comparables_names["bond_category"] = comparables_names["years"].apply(categorize_bond)


# %%
def categorize_bond_term(years):
    if years <= 0:
        return "Matured"
    elif years <= 4:
        return "short_term"
    elif years <= 12:
        return "medium_term"
    elif years <= 21:
        return "medium_long_term"
    elif years <= 30:
        return "long_term"
    elif years <= 38:
        return "very_long_term"
    else:
        return "super_ultra_long_term"


# Apply the function to create a new column
comparables_names["bond_term_category"] = comparables_names["years"].apply(
    categorize_bond_term
)

# %%
# validate if there are duplicates in id_isin
#print(comparables_names.shape)
#print(comparables_names["id_isin"].nunique())

# if shape and .nunique are the same show a message
#if comparables_names.shape[0] == comparables_names["id_isin"].nunique():
 #   print("There are no duplicates in id_isin")
#else:
 #   print("There are duplicates in id_isin")

# %%
# create a new column in case, that identifies whether a bond has ended or still has remaining time based on the 'years' column
comparables_names["bond_status"] = comparables_names["years"].apply(
    lambda x: "Ended" if x < 0 else "Remaining"
)

# %% [markdown]
# # Union Comparables Data Sets

# %%
# Melt comparables_ytm
comparables_ytm_melt = pd.melt(
    comparables_ytm, id_vars=["date"], var_name="id_isin", value_name="ytm"
)  # es útil para convertir las columnas en filas y así poder unirla de manera más eficiente con los nombres de los comparables y los isin
#print(comparables_ytm_melt.shape)

# %%
# drop where 'id_isin' = 'XS2523328479'
comparables_ytm_melt = comparables_ytm_melt[
    comparables_ytm_melt["id_isin"] != "XS2523328479"
]
#print(comparables_ytm_melt.shape)

# %%
# merge dataframes
final_comparables_ytm = comparables_ytm_melt.merge(
    comparables_names[
        [
            "id_isin",
            "issue_date",
            "ticker",
            "fitch_rating",
            "moody_rtg",
            "s&p_rating",
            "maturity_2",
            "years",
            "bond_category",
            "bond_term_category",
            "bond_status",
        ]
    ],
    on="id_isin",
    how="left",
)
#print(final_comparables_ytm.shape)

# %%
# to datetime
final_comparables_ytm["date"] = pd.to_datetime(final_comparables_ytm["date"])
final_comparables_ytm["issue_date"] = pd.to_datetime(
    final_comparables_ytm["issue_date"]
)
final_comparables_ytm["maturity_2"] = pd.to_datetime(
    final_comparables_ytm["maturity_2"]
)

# %% [markdown]
# # Descriptive Analysis

# %%
# Create a dictionary to map country names
countries_mapping = {
    "BRAZIL": "brasil",
    "CHILE": "chile",
    "COLOM": "colombia",
    "INDON": "indonesia",
    "PERU": "peru",
    "DOMREP": "rd",
    "ELSALV": "salvador",
    "COSTAR": "crc",
    "MEX": "mexico",
}

# %%
# Rename the 'maturity_2' column to 'maturity'
final_comparables_ytm.rename(columns={"maturity_2": "maturity"}, inplace=True)

# %%
# Replace the names in the ticker column using the mapping
final_comparables_ytm["country"] = final_comparables_ytm["ticker"].replace(
    countries_mapping
)

# %%
# convert to datitime 'date', 'issue_date' and 'maturity'
final_comparables_ytm["date"] = pd.to_datetime(final_comparables_ytm["date"])
final_comparables_ytm["maturity"] = pd.to_datetime(final_comparables_ytm["maturity"])
final_comparables_ytm["issue_date"] = pd.to_datetime(
    final_comparables_ytm["issue_date"]
)

# %%
# drop ticker column
final_comparables_ytm.drop(columns=["ticker"], inplace=True)

# %%
# order columns
order_columns = [
    "date",
    "id_isin",
    "ytm",
    "country",
    "issue_date",
    "maturity",
    "bond_status",
    "years",
    "bond_category",
    "bond_term_category",
    "fitch_rating",
    "moody_rtg",
    "s&p_rating",
]
final_comparables_ytm = final_comparables_ytm[order_columns]

# %% [markdown]
# # Anomality Check

# %%
# check NaN
final_comparables_ytm.isnull().sum()

# %%
# describe 'ytm' values diferent fom -999
final_comparables_ytm["ytm"].describe()

# %%
# Filtrar valores diferentes de -999 para los cálculos
valid_ytm = final_comparables_ytm[final_comparables_ytm["ytm"] != -999]["ytm"]

# %%
# Calcular Q1, Q3 e IQR para los valores válidos
Q1 = valid_ytm.quantile(0.25)
Q3 = valid_ytm.quantile(0.75)
IQR = Q3 - Q1

# %%
# Calcular los límites inferior y superior
lower_fence = Q1 - 1.5 * IQR
upper_fence = Q3 + 1.5 * IQR

# %%
# Crear una nueva columna 'is_anomaly' para identificar anomalías
final_comparables_ytm["is_anomaly"] = np.where(
    (final_comparables_ytm["ytm"] != -999)
    & (
        (final_comparables_ytm["ytm"] < lower_fence)
        | (final_comparables_ytm["ytm"] > upper_fence)
    ),
    1,  # Es una anomalía
    0,  # No es una anomalía
)

# %%
#print(f"Q1: {Q1}")
#print(f"Q3: {Q3}")
#print(f"IQR: {IQR}")
#print(f"Lower fence: {lower_fence}")
#print(f"Upper fence: {upper_fence}")

# %% [markdown]
# # Latin and Asia Final Set

# %%
# create a new dataframe with out anomalies
lat_bonds = final_comparables_ytm[final_comparables_ytm["is_anomaly"] == 0]
#print("lat_bonds shape ----->", final_comparables_ytm.shape)

# %%
# only ytm>0
lat_bonds = lat_bonds[lat_bonds["ytm"] > 0]

# %% [markdown]
# # Panama Global Final Set

# %%
# create a new dataframe with out anomalies for panama globales
panama_globales_bonds = panama_ytm_globales.copy()
panama_globales_bonds["ytm"] = panama_globales_bonds["ytm"].astype(float)
#print("panama_ytm_globales shape ----->", panama_ytm_globales.shape)

# %% [markdown]
# # Treasury Data Sets

# %%
# define path to data
base_path = "C:/Users/dsosa/Documents/snowflake_visual/data/external"  # change here if another user is running the code

# %%
# This function processes daily treasury rates data from CSV files for years 2014 to 2025.

# Create a dictionary to store DataFrames
ts_dict = {}  # Stores DataFrames representing the treasury rates for each year.

# Create a dictionary to store null value information
null_info = {}  # Records information about null values in the data.

# Loop through years from 14 to 25. The function then loops through years from 14 to 24 (representing 2014 to 2025)
for year in range(
    14, 26
):  # ------------------------------------------------------------------------------------------------>>>> Change here when new YEAR is added
    file_name = f"daily-treasury-rates_{year:02d}.csv"
    file_path = os.path.join(base_path, file_name)

    # Get the absolute path to the file
    file_path = os.path.abspath(file_path)

    # Read the CSV file
    df = pd.read_csv(file_path, delimiter=",", header=0, low_memory=False)

    # Remove spaces from column names
    df.columns = df.columns.str.replace(" ", "")

    # Convert 'Date' column to datetime and then to 'YYYY-MM-DD' format
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    # Check for null values before imputation
    null_counts_before = df.isnull().sum()

    # Perform imputation for all columns
    for col in df.columns:
        if df[col].dtype in ["float64", "int64"]:
            # For numeric columns, use mean imputation
            df[col] = df[col].fillna(df[col].mean())
        elif df[col].dtype == "object":
            # For object (string) columns, use mode imputation
            df[col] = df[col].fillna(
                df[col].mode()[0] if not df[col].mode().empty else "Unknown"
            )
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            # For datetime columns, use the most frequent value
            df[col] = df[col].fillna(
                df[col].value_counts().index[0]
                if not df[col].value_counts().empty
                else pd.NaT
            )

    # Check for null values after imputation
    null_counts_after = df.isnull().sum()

    # Calculate the number of imputed values
    imputed_counts = null_counts_before - null_counts_after

    # Store null value and imputation information
    null_info[f"ts_{year}"] = {
        "null_counts_before": null_counts_before,
        "imputed_counts": imputed_counts,
        "null_counts_after": null_counts_after,
    }

    # Store the DataFrame in the dictionary
    ts_dict[f"ts_{year}"] = df

# Print null value and imputation information for each DataFrame
#for year, info in null_info.items():
#    print(f"\nNull value and imputation information for {year}:")
#    print("Null counts before imputation:")
#    print(info["null_counts_before"])
#    print("\nNumber of null values imputed:")
#    print(info["imputed_counts"])
#    print("\nNull counts after imputation:")
#    print(info["null_counts_after"])
#    print("\n" + "=" * 50)

# %%
# Consolidate all DataFrames in ts_dict into a final DataFrame
final_ts_df = pd.concat(ts_dict.values(), ignore_index=True)

# %%
# Sort the final DataFrame by date
final_ts_df = final_ts_df.sort_values("Date").reset_index(drop=True)

# %%
# check with columns has null values
null_columns = final_ts_df.columns[final_ts_df.isnull().any()].tolist()
#print("Columns with null values:", null_columns)
#print(final_ts_df[null_columns].isnull().sum())

# %%
# drop columns with null values
final_ts_df = final_ts_df.drop(columns=null_columns)
#print(final_ts_df.shape)

# %%
# order columns
order_columns = [
    "Date",
    "1Mo",
    "3Mo",
    "6Mo",
    "1Yr",
    "2Yr",
    "3Yr",
    "5Yr",
    "7Yr",
    "10Yr",
    "20Yr",
    "30Yr",
]

# %%
# Define treasury main data set
treasury_data = final_ts_df[order_columns].copy()
#print('Treasury Data',treasury_data.shape)

# %%
# Melt the DataFrame
treasury_data_m = pd.melt(
    treasury_data, id_vars=["Date"], var_name="maturity", value_name="ytm"
)

# %%
# Create benchmark rates dataframe
benchmark_rates = pd.DataFrame(
    {
        "Date": treasury_data["Date"],
        "very_short_term": treasury_data["1Yr"],
        "short_term": treasury_data["2Yr"],
        "short_medium_term": treasury_data["3Yr"],
        "medium_term": treasury_data["5Yr"],
        "medium_long_term": treasury_data["7Yr"],
        "long_term": treasury_data["10Yr"],
        "very_long_term": treasury_data["20Yr"],
        "ultra_long_term": treasury_data["30Yr"],
        "super_ultra_long_term": treasury_data["30Yr"],  # Using 30Yr as proxy for >30Yr
    }
)

# %%
#print("Comparables columns:")
#print(lat_bonds.columns)
#print("\nBenchmark rates columns:")
#print(treasury_data_m.columns)

# %%
#print("Globales columns:")
#print(panama_globales_bonds.columns)
#print("\nBenchmark rates columns:")
#print(treasury_data_m.columns)

# %%
# Ensure date columns are in datetime format
lat_bonds["date"] = pd.to_datetime(lat_bonds["date"])
panama_globales_bonds["date"] = pd.to_datetime(panama_globales_bonds["date"])
treasury_data_m["Date"] = pd.to_datetime(treasury_data_m["Date"])
benchmark_rates["Date"] = pd.to_datetime(benchmark_rates["Date"])


# %%
def get_treasury_rate(row, benchmark_rates):
    """
    Retrieves the appropriate treasury rate for a given bond based on its date and term category.

    :param row: A row from the bonds dataframe containing 'date' and 'bond_term_category'
    :param benchmark_rates: A dataframe of treasury rates indexed by date
    :return: The corresponding treasury rate or np.nan if not applicable
    """
    date = row["date"]
    category = row["bond_term_category"]

    matching_rates = benchmark_rates[benchmark_rates["Date"] <= date]
    if matching_rates.empty:
        return np.nan

    if category == "Matured":
        return np.nan
    elif category == "short_term":
        return matching_rates["short_term"].iloc[-1]
    elif category == "medium_term":
        return matching_rates["medium_term"].iloc[-1]
    elif category == "medium_long_term":
        return matching_rates["medium_long_term"].iloc[-1]
    elif category == "long_term":
        return matching_rates["long_term"].iloc[-1]
    elif category == "very_long_term":
        return matching_rates["very_long_term"].iloc[-1]
    elif category == "super_ultra_long_term":
        return matching_rates["super_ultra_long_term"].iloc[-1]
    else:
        return np.nan


# %%
# Apply the function to get treasury rates for each bond
lat_bonds["treasury_rate"] = lat_bonds.apply(
    lambda row: get_treasury_rate(row, benchmark_rates), axis=1
)


# %%
# partition again before 'woe' study
def categorize_bond_term_globales(time_to_maturity_rounded):
    if time_to_maturity_rounded <= 0:
        return "Matured"
    elif time_to_maturity_rounded <= 4:
        return "short_term"
    elif time_to_maturity_rounded <= 12:
        return "medium_term"
    elif time_to_maturity_rounded <= 21:
        return "medium_long_term"
    elif time_to_maturity_rounded <= 30:
        return "long_term"
    elif time_to_maturity_rounded <= 38:
        return "very_long_term"
    else:
        return "super_ultra_long_term"


# Apply the function to create a new column
panama_globales_bonds["bond_term_category"] = panama_globales_bonds[
    "time_to_maturity_rounded"
].apply(categorize_bond_term_globales)

# %%
# Apply the function to get treasury rates for each bond
panama_globales_bonds["treasury_rate"] = panama_globales_bonds.apply(
    lambda row: get_treasury_rate(row, benchmark_rates), axis=1
)

# %% [markdown]
# # Create Spread over Treasury

# %%
# create a year column
lat_bonds["year"] = lat_bonds["date"].dt.year

# %%
# Calculate the benchmark spread
lat_bonds["benchmark_spread"] = lat_bonds["ytm"] - lat_bonds["treasury_rate"]

# %%
# Calculate the benchmark spread
panama_globales_bonds["benchmark_spread"] = (
    panama_globales_bonds["ytm"] - panama_globales_bonds["treasury_rate"]
)

# %%
# add panama to lat_bonds
# create a new column named 'country' with value panama
panama_globales_bonds["country"] = "panama"

# %%
# count bond_term_category
panama_globales_bonds["bond_term_category"].value_counts()

# %%
# Set a seed for reproducibility
from datetime import datetime, timedelta

np.random.seed(42)
# Define the date range for the random dates
start_date = datetime(2024, 1, 1)
end_date = datetime(2034, 12, 31)


# Function to generate random dates
def random_date(start, end, n):
    delta = end - start
    random_days = np.random.randint(0, delta.days, n)
    return [start + timedelta(days=int(day)) for day in random_days]


# Generate random maturity dates
panama_globales_bonds["maturity"] = random_date(
    start_date, end_date, len(panama_globales_bonds)
)

# %%
# create a new column named 'country' with value panama
panama_globales_bonds["bond_status"] = "Remaining"

# %%
# Rename columns in df_pan to match la_bonds
panama_ytm = panama_globales_bonds.rename(
    columns={"fecha_de_tx": "date", "Isin": "id_isin"}
)

# %%
# order same columns for both datasets
lat_bonds = lat_bonds[
    [
        "date",
        "country",
        "id_isin",
        "ytm",
        "treasury_rate",
        "benchmark_spread",
        "bond_term_category",
    ]
]
panama_ytm = panama_ytm[
    [
        "date",
        "country",
        "id_isin",
        "ytm",
        "treasury_rate",
        "benchmark_spread",
        "bond_term_category",
    ]
]

# %%
# to datetime
lat_bonds["date"] = pd.to_datetime(lat_bonds["date"]).copy()
panama_ytm["date"] = pd.to_datetime(panama_ytm["date"]).copy()

# %%
# Drop duplicates based on all columns, keeping the first occurrence
lat_bonds = lat_bonds.drop_duplicates(keep="first")
panama_ytm = panama_ytm.drop_duplicates(subset=["id_isin", "ytm"], keep="first")

# %%
# Concatenate the two dataframes
# NOTA: OJO, EN LA DATA DE PANAMA LOS ÚNICOS DATOS VÁLIDOS SON('date', 'country', 'id_isin', 'ytm', 'treasury_rate', 'benchmark_spread')
panama_comparables_final = pd.concat([lat_bonds, panama_ytm], ignore_index=True)
# print(panama_comparables_final.shape)

# %%
# Step 1: Create a numerical mask for bond_term_category
term_mapping = {
    "Matured": 0,
    "short_term": 1,
    "medium_term": 2,
    "medium_long_term": 3,
    "long_term": 4,
    "very_long_term": 5,
}
panama_comparables_final["term_mask"] = panama_comparables_final[
    "bond_term_category"
].map(term_mapping)

# %%
# Create the inverse mapping
term_mapping_inv = {v: k for k, v in term_mapping.items()}

# %%
# Step 2: Pivot the table
pivoted_df = panama_comparables_final.pivot_table(
    index="date",
    columns=["country", "term_mask"],
    values="benchmark_spread",
    aggfunc="mean",
)

# %%
# Step 3: Flatten the multi-index columns and add suffix
pivoted_df.columns = [
    f"{country}_{term_mapping_inv[term]}" for country, term in pivoted_df.columns
]

# %%
# Reset index for cleaner look
pivoted_df.reset_index(inplace=True)

# %% [markdown]
# # Define Short-term data

# %%
# short_term
short_term_columns = [
    col
    for col in pivoted_df.columns
    if "_short_term" in col and col != "panama_short_term"
]
short_term_columns = ["date"] + short_term_columns + ["panama_short_term"]
panama_short_term_df = pivoted_df[short_term_columns]

#print("panama_short_term_df:", panama_short_term_df.shape)

# %%
# head
print("Final file to export to Snowflake", panama_short_term_df.head())

#print the final date of the data
print("Final date of the data:", panama_short_term_df["date"].max())

# %%
# to csv here
panama_short_term_df.to_csv(
    "C:/Users/dsosa/Documents/snowflake_visual/data/processed/panama_short_term.csv",
    index=False,
)  # change here if another user is running the code
