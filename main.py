import asyncio
import uvloop
from fastapi import FastAPI
from fastapi import WebSocket, WebSocketDisconnect
from starlette.middleware.cors import CORSMiddleware

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _init():
    global ws_pool
    from util.cache import WebsocketPool, AsyncRedis
    ws_pool = WebsocketPool(AsyncRedis('127.0.0.1', 6379, '', 0))
    asyncio.get_event_loop().create_task(ws_pool.start_listening())


@app.get("/health")
def read_root():
    return {"health": "ok"}


class WebsocketConnection(object):
    def __init__(self, websocket: WebSocket, user_id):
        self.websocket = websocket
        ws_pool.add_connection(user_id, websocket)

    async def get_msgs(self):
        try:
            while True:
                msg_json = await self.websocket.receive_json()
                yield msg_json
        except Exception as e:
            print(e)

    async def msg_loop(self):
        async for msg in self.get_msgs():
            try:
                # await self.websocket.send_json(msg)
                await ws_pool.publish_msg(msg)
            except Exception as e:
                print(e)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, user_id):
    await websocket.accept()
    conn = WebsocketConnection(websocket, user_id)
    await conn.msg_loop()


if __name__ == "__main__":
    from gunicorn.app.base import BaseApplication


    class GunicornApp(BaseApplication):
        def __init__(self, app, options=None):
            self.application = app
            self.options = options or {}
            super().__init__()

        def load_config(self):
            config = {key: value for key, value in self.options.items()
                      if key in self.cfg.settings and value is not None}
            for key, value in config.items():
                self.cfg.set(key.lower(), value)

        def load(self):
            return self.application


    options = {
        'bind': '0.0.0.0:8000',
        'workers': 3,
        'threads': 32,
        'worker_class': 'uvicorn.workers.UvicornWorker',
        'timeout': 60 * 5,
    }
    GunicornApp(app, options).run()
