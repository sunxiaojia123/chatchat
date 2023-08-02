import json

from redis import asyncio as aioredis
import asyncio


class AsyncRedis:
    def __init__(self, host, port, password, db, pool_size: int = 10):
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.pool_size = pool_size
        self.redis_store = None

    async def initialize(self):
        pool = aioredis.ConnectionPool(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            max_connections=self.pool_size,
            encoding="utf-8",
            decode_responses=True,
        )
        self.redis_store = aioredis.Redis(connection_pool=pool)
        return self

    async def get_redis(self):
        if self.redis_store is None:
            await self.initialize()
        else:
            try:
                await self.redis_store.ping()
            except (aioredis.ConnectionError, aioredis.TimeoutError):
                await self.initialize()
        return self


class SubChannel:
    def __init__(self, redis: AsyncRedis, channel='message_channel_http'):
        self.channel = self.get_channel(channel)
        self.redis = redis

    def get_channel(self, channel):
        return self.__class__.__name__ + ":" + channel

    async def _pub_sub_init(self):
        self.redis = await self.redis.get_redis()
        self.pubsub = self.redis.redis_store.pubsub()
        await self.pubsub.subscribe(self.channel)
        return self.pubsub

    async def _sub_listen(self, handleFunc):
        print('sub listen')
        await self._pub_sub_init()
        while True:
            try:
                print('sub listen while')
                _, _, data = await self.pubsub.parse_response()
                data = json.loads(data)
                print(data)
                await handleFunc(data)
            except (aioredis.ConnectionError, aioredis.TimeoutError):
                await self._pub_sub_init()
                await asyncio.sleep(1)
            except TypeError:
                pass
            except Exception as e:
                print(e)
                await asyncio.sleep(1)

    async def publish_msg(self, data):
        self.redis = await self.redis.get_redis()
        await self.redis.redis_store.execute_command('publish', self.channel, json.dumps(data))

    async def close(self):
        self.pubsub.close()


class WebsocketPool(SubChannel):
    connections = {}

    def add_connection(self, key, websocket):
        self.connections[key] = websocket

    def remove_connection(self, key):
        del self.connections[key]

    async def send_message(self, key, data):
        await self.connections[key].send_json(data)

    async def message_handle(self, data):
        await self.send_message(data['key'], data['data'])

    def __init__(self, redis: AsyncRedis):
        super().__init__(redis=redis)
        asyncio.get_event_loop().create_task(self._sub_listen(self.message_handle))


ws_pool = WebsocketPool(AsyncRedis('127.0.0.1', 6379, '', 0))
