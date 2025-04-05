import asyncio
import logging
from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
from typing import Callable
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

class ConsumerService:
    def __init__(
        self,
        topic_name: str,
        bootstrap_servers: str,
        sasl_mechanism: str,
        security_protocol: str,
        sasl_plain_username: str,
        sasl_plain_password: str,
        loop: asyncio.AbstractEventLoop,
        group_id: str = None,
        auto_offset_reset: str = "latest"
    ):
        """
        Store 'loop' so we can schedule coroutines on it from our sync context.
        """
        self.loop = loop
        self.running = True
        self.executor = ThreadPoolExecutor(max_workers=10)

        self.consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            sasl_mechanism=sasl_mechanism,
            security_protocol=security_protocol,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            max_poll_interval_ms=300000,
            session_timeout_ms=30000,
            fetch_max_wait_ms=500,
            max_poll_records=500,
            fetch_max_bytes=10 * 1024 * 1024,
            max_partition_fetch_bytes=5 * 1024 * 1024,
            value_deserializer=self._json_deserializer
        )

    @staticmethod
    def _json_deserializer(data: bytes):
        try:
            return json.loads(data.decode("utf-8"))
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error: {e}")
            return None

    def consume_message(self, func: Callable):
        """
        This is still a synchronous method, but we can schedule
        async calls on the event loop when we get messages.
        """
        logging.info("Kafka consumer started.")
        try:
            while self.running:
                records = self.consumer.poll(timeout_ms=500, max_records=500)
                for _, msgs in records.items():
                    for message in msgs:
                        if message.value is not None:
                            # Instead of calling func(...) directly (which is async),
                            # we schedule it on the event loop:
                            future = asyncio.run_coroutine_threadsafe(
                                func(message.value),  # 'func' must be async
                                self.loop
                            )
                            # Optionally handle 'future' if you want to track results
        except Exception as e:
            logging.error(f"Consumer error: {e}")
        finally:
            self.stop()
            logging.info("Kafka consumer stopped.")

    def stop(self):
        self.running = False
        self.executor.shutdown(wait=True)
        if self.consumer:
            self.consumer.close()
            logging.info("Kafka consumer closed successfully.")
