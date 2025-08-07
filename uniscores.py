
import json
import requests
import base64
from datetime import datetime
import pytz
import sys
import base64
import urllib3
import json
from collections import Counter, defaultdict
import os

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global variables - Configure these values
METABASE_URL = os.environ.get("METABASE_URL")
METABASE_API_TOKEN = os.environ.get("METABASE_API_TOKEN")
sesh = ''
headers = {
        'Content-Type': 'application/json'
    }

def get_completed_scp(question_id):
    
    headers = {
        "x-api-key": METABASE_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    # Get results in JSON format
    endpoint = f"{METABASE_URL}/api/card/{question_id}/query/json"
    response = requests.post(endpoint, headers=headers, verify=False)
    
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to get results: {response.text}")

scores = get_completed_scp(238)


EVENT_BASE_URL = "https://portal-eu.simspace.com/index.html#/training/catalog/structured-content-plan/"

#print(scores[0])

import pandas as pd
import json
from collections import defaultdict

# === Load challenge data exported from your SQL ===
df = pd.DataFrame(scores)

# Filter out entries without any earned points (i.e., unranked)
df['challenge_points_earned'] = df['challenge_points_earned'].fillna(0)

df['challenge_points_earned'] = df['challenge_points_earned'].astype(int)
df['challenge_points_possible'] = df['challenge_points_possible'].astype(int)

# === User Leaderboard ===
user_grouped = (
    df.groupby(['email', 'full_name', 'university'])['challenge_points_earned']
    .sum()
    .reset_index()
    .rename(columns={'challenge_points_earned': 'total_points'})
)

user_grouped['total_points'] = pd.to_numeric(user_grouped['total_points'], errors='coerce').fillna(0)
user_grouped = user_grouped.sort_values(by='total_points', ascending=False)

users = user_grouped.drop(columns=['email'], errors='ignore').to_dict(orient='records')

# === University Leaderboard ===
univ_grouped = (
    df[df['challenge_points_earned'] > 0]
    .groupby('university')
    .agg(
        total_points=('challenge_points_earned', 'sum'),
        num_users=('email', 'nunique')
    )
    .reset_index()
)

univ_grouped['total_points'] = pd.to_numeric(univ_grouped['total_points'], errors='coerce').fillna(0)
univ_grouped = univ_grouped.sort_values(by='total_points', ascending=False)

universities = univ_grouped.to_dict(orient='records')

# === Package Tiles with Release Date & School Stats ===
packages_dict = defaultdict(lambda: {
    'package_id': '',
    'package_name': '',
    'difficulty': '',
    'passing_threshold': 0,
    'release_date': '',
    'universities': defaultdict(int),
    'users': []
})

for _, row in df.iterrows():
    pkg = packages_dict[row['package_id']]
    pkg['package_id'] = row['package_id']
    pkg['package_name'] = row['package_name']
    pkg['difficulty'] = row['difficulty']
    pkg['passing_threshold'] = row['passing_threshold']
    pkg['release_date'] = row['release_date']

    # Only increment university stats if points were earned and university is known
    if row['challenge_points_earned'] > 0 and pd.notnull(row['university']):
        pkg['universities'][row['university']] += 1

    # Track user-level info if user scored
    if row['challenge_points_earned'] > 0:
        pkg['users'].append({
            'full_name': row['full_name'],
            'challenge_points_earned': int(row['challenge_points_earned'])
        })

# Final flatten of packages
packages = []
for pkg in packages_dict.values():
    pkg['universities'] = [
        {'name': uni, 'users': count} for uni, count in pkg['universities'].items()
    ]
    packages.append(pkg)

# === Optional: Derive difficulty_points and rank_multipliers from actual data ===
difficulty_points = df.dropna(subset=['challenge_points_possible']).groupby('difficulty')['challenge_points_possible'].max().to_dict()
rank_multipliers = {}

# Avoid divide-by-zero when possible points are 0
valid_rows = df[(df['challenge_points_possible'] > 0) & (df['challenge_points_earned'] > 0)]

for rank in valid_rows['rank'].dropna().unique():
    ranks_df = valid_rows[valid_rows['rank'] == rank]
    multipliers = ranks_df['challenge_points_earned'] / ranks_df['challenge_points_possible']
    rank_multipliers[int(rank)] = round(multipliers.mean(), 2)

# Add default multiplier for non-top-3 if present
if 0.1 in (valid_rows['challenge_points_earned'] / valid_rows['challenge_points_possible']).values:
    rank_multipliers['other'] = 0.1

# === Final Output ===
leaderboard_data = {
    'users': users,
    'universities': universities,
    'packages': packages,
    'difficulty_points': difficulty_points,
    'rank_multipliers': rank_multipliers
}

with open('training_winners_data.json', 'w') as f:
    json.dump(leaderboard_data, f, indent=2)

print("✅ training_winners_data.json generated successfully.")
