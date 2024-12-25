# region imports dependencies
from aiokafka import AIOKafkaConsumer
from typing import Callable
import json
import asyncio

# endregion

# region consumer_service
class Consumerservice:
    def __init__(self, topic_name, bootstrap_servers, sasl_mechanism, security_protocol, sasl_plain_username, sasl_plain_password, group_id=None, auto_offset_reset='latest'):
        self.topic_name = topic_name
        self.bootstrap_servers = bootstrap_servers
        self.sasl_mechanism = sasl_mechanism
        self.security_protocol = security_protocol
        self.sasl_plain_username = sasl_plain_username
        self.sasl_plain_password = sasl_plain_password
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.consumer = None

    async def start_consumer(self):
        self.consumer = AIOKafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.bootstrap_servers,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        await self.consumer.start()
        
    async def consume_message(self, func: Callable):
        try:
            async for message in self.consumer:
                asyncio.create_task(func(message.value))
        finally:
            await self.consumer.stop()

# endregion