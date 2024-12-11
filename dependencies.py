from dotenv import load_dotenv
import os

load_dotenv()

bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVER")
sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM")
sasl_plain_username=os.getenv("KAFKA_CLIENT_USERNAME")
sasl_plain_password=os.getenv("KAFKA_CLIENT_PASSWORD")
security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL")