import pandas as pd
import time

# Paths to input files
bs_csv_path = "C:\\Users\\Mateu\\Desktop\\Semestr5\\BDlab\\1m\\fraud\\copy\\bs.csv"
bsnet_csv_path = "C:\\Users\\Mateu\\Desktop\\Semestr5\\BDlab\\1m\\fraud\\copy\\bsNET.csv"

# Load the CSV files
print("Loading datasets...")
start_time = time.time()
bs_df = pd.read_csv(bs_csv_path)
bsnet_df = pd.read_csv(bsnet_csv_path)
load_time = time.time() - start_time
print(f"Datasets loaded in {load_time:.2f} seconds")

# 1. Count transactions per customer
print("Calculating transaction count per customer...")
start_time = time.time()
transaction_count = bs_df.groupby("customer").size().reset_index(name="transaction_count")
transaction_count.to_csv("transaction_count.csv", index=False)
task_time = time.time() - start_time
print(f"Transaction count aggregation completed in {task_time:.2f} seconds")

# 2. Total amount spent per category
print("Calculating total amount spent per category...")
start_time = time.time()
total_spent_per_category = bs_df.groupby("category")["amount"].sum().reset_index(name="total_spent")
total_spent_per_category.to_csv("total_spent_per_category.csv", index=False)
task_time = time.time() - start_time
print(f"Total spent per category aggregation completed in {task_time:.2f} seconds")

# 3. Average transaction amount for each gender
print("Calculating average transaction amount per gender...")
start_time = time.time()
average_amount_per_gender = bs_df.groupby("gender")["amount"].mean().reset_index(name="average_amount")
average_amount_per_gender.to_csv("average_amount_per_gender.csv", index=False)
task_time = time.time() - start_time
print(f"Average transaction amount per gender aggregation completed in {task_time:.2f} seconds")

# 4. Join bs and bsNET for detailed data
print("Joining bs and bsNET datasets...")
start_time = time.time()
detailed_data = bs_df.merge(bsnet_df, left_on="customer", right_on="Source", how="inner")
detailed_data.to_csv("detailed_transaction_data.csv", index=False)
task_time = time.time() - start_time
print(f"Detailed transaction data creation completed in {task_time:.2f} seconds")

# 5. Transactions where customer and merchant ZIP codes differ
print("Identifying ZIP code mismatches...")
start_time = time.time()
zip_mismatch = bs_df[bs_df["zipcodeOri"] != bs_df["zipMerchant"]]
zip_mismatch.to_csv("zip_mismatch.csv", index=False)
task_time = time.time() - start_time
print(f"ZIP mismatch data creation completed in {task_time:.2f} seconds")

# 6. Total money spent per customer and their network weight
print("Calculating total money spent and network weight per customer...")
start_time = time.time()
total_spent_and_weight = (
    bs_df.merge(bsnet_df, left_on="customer", right_on="Source", how="inner")
    .groupby("customer")
    .agg(total_spent=("amount", "sum"), total_weight=("Weight", "sum"))
    .reset_index()
)
total_spent_and_weight.to_csv("total_spent_and_weight.csv", index=False)
task_time = time.time() - start_time
print(f"Total spent and weight aggregation completed in {task_time:.2f} seconds")
