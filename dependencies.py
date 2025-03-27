from dotenv import load_dotenv
from Services.LoadIndex import load_csv_files_to_pandas, PREDEFINED_WATCHLIST, INDEX_FILENAME
import os
import pandas as pd

load_dotenv()

bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVER")
sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM")
sasl_plain_username=os.getenv("KAFKA_CLIENT_USERNAME")
sasl_plain_password=os.getenv("KAFKA_CLIENT_PASSWORD")
security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL")

pg_username=os.getenv("POSTGRES_USERNAME")
pg_password=os.getenv("POSTGRES_PASSWORD")
pg_host=os.getenv("POSTGRES_HOST")
pg_port=os.getenv("POSTGRES_PORT")
pg_db=os.getenv("POSTGRES_DB")

SECTOR_SYMBOLS = load_csv_files_to_pandas()

df = pd.DataFrame(SECTOR_SYMBOLS)

SECTOR_SYMBOLS_DF = df.drop_duplicates().set_index(["Symbol"]).dropna(subset=['Industry'])

industries = [item["Industry"] for item in SECTOR_SYMBOLS]

UNIQUE_INDUSTRIES = []
seen = set()
for industry in industries:
    if industry not in seen:
        seen.add(industry)
        UNIQUE_INDUSTRIES.append(industry)