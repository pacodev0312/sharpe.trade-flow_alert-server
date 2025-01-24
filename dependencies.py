from dotenv import load_dotenv
from Services.LoadIndex import load_csv_files_to_pandas
import os

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

index_dict = load_csv_files_to_pandas()