import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import os
import glob

# -----------------------------
# Configuration
# -----------------------------
folders = ["data/users", "data/movies", "data/watch_history", "data/ratings"]
for folder in folders:
    os.makedirs(folder, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M")

# -----------------------------
# Load existing data if any
# -----------------------------
def load_latest_csv(folder, id_col):
    files = sorted(glob.glob(f"{folder}/*.csv"))
    if files:
        df = pd.read_csv(files[-1])
        last_id = df[id_col].max()
        return df, last_id
    else:
        return pd.DataFrame(), 0

users_df, last_user_id = load_latest_csv("data/users", "user_id")
movies_df, last_movie_id = load_latest_csv("data/movies", "movie_id")
watch_df, last_watch_id = load_latest_csv("data/watch_history", "watch_id")

rating_files = sorted(glob.glob("data/ratings/*.json"))
if rating_files:
    with open(rating_files[-1], "r") as f:
        ratings_list = json.load(f)
    last_rating_id = max(r["rating_id"] for r in ratings_list)
else:
    ratings_list = []
    last_rating_id = 0

# -----------------------------
# Determine first run or delta run
# -----------------------------
if last_user_id == 0:
    # First run
    num_new_users = 100
    num_new_movies = 100
    num_new_watch = 300
    num_new_ratings = 200
else:
    # Subsequent daily run
    num_new_users = 10
    num_new_movies = 10
    num_new_watch = 20
    num_new_ratings = 15

# -----------------------------
# Generate new users
# -----------------------------
new_users = pd.DataFrame({
    "user_id": range(last_user_id + 1, last_user_id + 1 + num_new_users),
    "name": [f"User_{i}" for i in range(last_user_id + 1, last_user_id + 1 + num_new_users)],
    "age": np.random.randint(18, 65, size=num_new_users),
    "gender": np.random.choice(["Male", "Female", "Other"], size=num_new_users),
    "country": np.random.choice(["US", "UK", "CA", "IN", "AU"], size=num_new_users),
    "signup_date": [datetime.now() for _ in range(num_new_users)]
})
users_df = pd.concat([users_df, new_users], ignore_index=True)
users_file = f"data/users/users_{timestamp}.csv"
users_df.to_csv(users_file, index=False)

# -----------------------------
# Generate new movies
# -----------------------------
new_movies = pd.DataFrame({
    "movie_id": range(last_movie_id + 1, last_movie_id + 1 + num_new_movies),
    "title": [f"Movie_{i}" for i in range(last_movie_id + 1, last_movie_id + 1 + num_new_movies)],
    "genre": np.random.choice(["Action", "Comedy", "Drama", "Horror", "Sci-Fi"], size=num_new_movies),
    "language": np.random.choice(["English", "Spanish", "French", "Hindi"], size=num_new_movies),
    "release_year": np.random.randint(1990, 2025, size=num_new_movies)
})
movies_df = pd.concat([movies_df, new_movies], ignore_index=True)
movies_file = f"data/movies/movies_{timestamp}.csv"
movies_df.to_csv(movies_file, index=False)

# -----------------------------
# Generate new watch history
# -----------------------------
new_watch = pd.DataFrame({
    "watch_id": range(last_watch_id + 1, last_watch_id + 1 + num_new_watch),
    "user_id": np.random.choice(users_df["user_id"], size=num_new_watch),
    "movie_id": np.random.choice(movies_df["movie_id"], size=num_new_watch),
    "watch_date": [datetime.now() - timedelta(days=np.random.randint(0, 7)) for _ in range(num_new_watch)],
    "watch_duration_minutes": np.random.randint(10, 180, size=num_new_watch),
    "device_type": np.random.choice(["Mobile", "Tablet", "Desktop", "Smart TV"], size=num_new_watch)
})
watch_df = pd.concat([watch_df, new_watch], ignore_index=True)
watch_file = f"data/watch_history/watch_history_{timestamp}.csv"
watch_df.to_csv(watch_file, index=False)

# -----------------------------
# Generate new ratings
# -----------------------------
new_ratings = []
for i in range(last_rating_id + 1, last_rating_id + 1 + num_new_ratings):
    rating = {
        "rating_id": i,
        "user_id": int(np.random.choice(users_df["user_id"])),
        "movie_id": int(np.random.choice(movies_df["movie_id"])),
        "rating": round(np.random.uniform(1, 5), 1),
        "rating_date": (datetime.now() - timedelta(days=np.random.randint(0, 7))).isoformat()
    }
    new_ratings.append(rating)
ratings_list.extend(new_ratings)
ratings_file = f"data/ratings/ratings_{timestamp}.json"
with open(ratings_file, "w") as f:
    json.dump(ratings_list, f, indent=4)

# -----------------------------
# Summary
# -----------------------------
print("Generated files:")
print(users_file)
print(movies_file)
print(watch_file)
print(ratings_file)
