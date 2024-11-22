import pandas as pd
import time

# Load CSV files
games_df = pd.read_csv("C:\\Users\\Mateu\\Desktop\\Semestr5\\BDlab\\10m\\data\\games.csv")
recommendations_df = pd.read_csv("C:\\Users\Mateu\\Desktop\\Semestr5\\BDlab\\10m\\data\\recommendations.csv")
users_df = pd.read_csv("C:\\Users\\Mateu\\Desktop\\Semestr5\\BDlab\\10m\\data\\users.csv")

# Timing and executing each aggregation/join separately

# === AGGREGATION QUERIES ===

# 1. Count the number of games by rating
start_time = time.time()
rating_game_count = games_df.groupby("rating").size().reset_index(name="game_count")
rating_game_count.to_csv("rating_game_count.csv", index=False)
end_time = time.time()
print(f"Aggregation 1 (Game count by rating) completed in {end_time - start_time:.2f} seconds.")

# 2. Get the average number of hours spent on each game (by app_id)
start_time = time.time()
avg_hours_per_game = recommendations_df.groupby("app_id")["hours"].mean().reset_index(name="avg_hours")
avg_hours_per_game.to_csv("avg_hours_per_game.csv", index=False)
end_time = time.time()
print(f"Aggregation 2 (Average hours per game) completed in {end_time - start_time:.2f} seconds.")

# 3. Total number of products owned by users with more than 1 review
start_time = time.time()
users_with_reviews = users_df[users_df["reviews"] > 1]
total_products_by_users = users_with_reviews.groupby("user_id")["products"].sum().reset_index(name="total_products")
total_products_by_users.to_csv("total_products_by_users.csv", index=False)
end_time = time.time()
print(f"Aggregation 3 (Total products for users with >1 review) completed in {end_time - start_time:.2f} seconds.")

# === JOIN QUERIES ===

# 4. List all games with the average hours spent on them (JOIN games and recommendations)
start_time = time.time()
games_with_avg_hours = pd.merge(
    games_df[["app_id", "title"]],
    avg_hours_per_game,
    on="app_id",
    how="inner"
)
games_with_avg_hours.to_csv("games_with_avg_hours.csv", index=False)
end_time = time.time()
print(f"Join 1 (Games with average hours) completed in {end_time - start_time:.2f} seconds.")

# 5. Users with the number of reviews they have written (JOIN users and recommendations)
start_time = time.time()
user_review_count = recommendations_df.groupby("user_id").size().reset_index(name="review_count")
users_with_review_count = pd.merge(
    users_df,
    user_review_count,
    on="user_id",
    how="inner"
)
users_with_review_count.to_csv("users_with_review_count.csv", index=False)
end_time = time.time()
print(f"Join 2 (Users with their review count) completed in {end_time - start_time:.2f} seconds.")

# 6. Find all games recommended by users owning more than 10 products (JOIN games, recommendations, and users)
start_time = time.time()
users_with_many_products = users_df[users_df["products"] > 10]
recommendations_filtered = recommendations_df[recommendations_df["is_recommended"] == True]

games_recommended_by_users = pd.merge(
    pd.merge(
        games_df[["app_id", "title"]],
        recommendations_filtered[["app_id", "user_id"]],
        on="app_id",
        how="inner"
    ),
    users_with_many_products[["user_id"]],
    on="user_id",
    how="inner"
)
games_recommended_by_users.to_csv("games_recommended_by_users.csv", index=False)
end_time = time.time()
print(f"Join 3 (Recommended games by users with >10 products) completed in {end_time - start_time:.2f} seconds.")

