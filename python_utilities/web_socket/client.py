import os
import time
import threading
import asyncio
from typing import Union

import websockets
from websockets.legacy.client import WebSocketClientProtocol

from logger import CustomLogger


logger = CustomLogger(__name__)


SERVER_HOST: str = "localhost"
SERVER_PORT: int = 7890
CLIENT_ID: str = f"nfer_llm_pid-{os.getpid()}"
QUERY_PARAM_CLIENT_ID: str = "client_id"
MESSAGES_ENDPOINT: str = "/messages"
RECEIVER_ID: str = "receiver_id"


class WebSocketClient:
    def __init__(
        self,
        client_id: str,
        host: str = SERVER_HOST,
        port: int = SERVER_PORT,
        max_retries: int = 3,
    ):
        self.client_id = client_id
        self.host = host
        self.port = port
        self.max_retries = max_retries

        self.connection: Union[WebSocketClientProtocol, None] = None
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.start_loop, daemon=True)
        self.thread.start()
        time.sleep(1) # wait for the connection to be established

    def start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.connect())

    async def connect(self):
        counter = 0
        while True:
            counter += 1
            try:
                query_params = f"?{QUERY_PARAM_CLIENT_ID}={self.client_id}"
                uri = f"ws://{self.host}:{self.port}" + MESSAGES_ENDPOINT + query_params
                self.connection: WebSocketClientProtocol = await websockets.connect(uri)
                logger.info("Connected to the websocket server", {"uri": uri})

                await self.connection.wait_closed()

                if self.connection.close_code == 1000:
                    break

                if self.connection.close_code == 4000:
                    logger.error(
                        message="Websocket Connection closed",
                        fields={"Error": self.connection.close_reason},
                    )

                logger.info("Websocket Connection closed. Retrying...")
            except Exception as e:
                logger.info(
                    message=f"Websocket Connection error. Retrying in 5 seconds...",
                    fields={"Error": repr(e)},
                )
                time.sleep(5)

            if counter == self.max_retries:
                logger.error(
                    message=(
                        "Failed to connect with websocket server. "
                        "Max retries reached. Exiting..."
                    )
                )
                break

        # wait until all pending tasks (except this) are done
        tasks = [
            t for t in asyncio.all_tasks(self.loop) if t is not asyncio.current_task()
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # setting connection to avoid unexpected behaviour
        self.connection = None

    def send(self, message: str):
        future = asyncio.run_coroutine_threadsafe(self._send(message), self.loop)
        result = future.result()
        return result

    async def _send(self, message: str):
        if self.connection:
            await self.connection.send(message)

    def receive(self):
        return asyncio.run_coroutine_threadsafe(self._receive(), self.loop).result()

    async def _receive(self):
        if self.connection:
            return await self.connection.recv()

    def close(self):
        asyncio.run_coroutine_threadsafe(self._close(), self.loop)

    async def _close(self):
        if self.connection:
            await self.connection.close()


if __name__ == "__main__":
    import json
    CLIENT_ID = "nfer_llm"
    ws_client = WebSocketClient(client_id=CLIENT_ID)
    time.sleep(2)

    receiver = "client_1"
    print(f"Sending to {receiver}")
    data = {
        "msg": f"Hello {receiver} from {CLIENT_ID}",
        "receiver_id": receiver,
    }
    ws_client.send(json.dumps(data))
    print(f"Sent to {receiver}")
    
    data = {
        "msg": f"Hello {receiver} from {CLIENT_ID} again ....",
        "receiver_id": receiver,
    }
    ws_client.send(json.dumps(data))

    print("waiting to receive...")
    result = ws_client.receive()
    print("Result: ", result)
    
    receiver = "client_2"
    print(f"Sending to {receiver}")
    data = {
        "msg": f"Hello {receiver} from {CLIENT_ID}",
        "receiver_id": receiver,
    }
    ws_client.send(json.dumps(data))
    print(f"Sent to {receiver}")

    print("Closing connection....")
    ws_client.close()
    ws_client.thread.join()
    print("thread finished...exiting")

