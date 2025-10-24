import os
from datetime import datetime

# Twitter API token
BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA")


POSTGRES_CONFIG = {
    "host": "forage-dev-db.cod4levdfbtz.ap-south-1.rds.amazonaws.com",
    "port": 5432,
    "database": "Kapow",
    "user": "kapow",
    "password": "kapow123"
}

#
# SAVE_FOLDER = fr'C:\Users\SALONI\OneDrive\jsonPagesave'
# os.makedirs(SAVE_FOLDER, exist_ok=True)


def get_output_table(base_name: str):
    date_str = datetime.now().strftime("%Y%m%d")
    return f"{base_name}_{date_str}"
