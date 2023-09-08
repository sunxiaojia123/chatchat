import asyncio
import uvloop
from fastapi import FastAPI, Body
from fastapi import WebSocket
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
    from ws_pool import WebsocketPool, AsyncRedis
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


@app.post("/mag")
async def send_msg(
        key: str = Body(...),
        data: any = Body(...)
):
    msg = {
        "key": key,
        "data": data
    }
    await ws_pool.publish_msg(msg)
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app="main:app", host="0.0.0.0", port=8000, workers=3, loop="uvloop")
