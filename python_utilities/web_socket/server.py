import json
import time
import asyncio
import threading
import multiprocessing
from typing import List, Union, Dict, Any, Callable
from urllib.parse import urlparse, parse_qs

import websockets
from websockets.legacy.server import Serve, WebSocketServerProtocol
from websockets.exceptions import ConnectionClosed


from logger import CustomLogger


logger = CustomLogger(__name__)

# Constants used in this file
CLIENT_ID: str = "client_id"
RECEIVER_ID: str = "receiver_id"


# Endpoints
MESSAGES_ENDPOINT: str = "/messages"


# Default values
DEFAULT_HOST: str = "localhost"
DEFAULT_PORT: int = 7890
MINUTES_TO_CLEAR_BUFFER: int = 30


# Memory buffer keys
CREATED_AT: str = "created_at"
MESSAGES: str = "messages"


class WebSocketServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

        # stores a map of client ids to their respective websocket connections
        self.id_to_client_map: Dict[str, WebSocketServerProtocol] = {}

        """
        A temporary memory buffer to store messages for a client if the client is not
        connected. The messages will be sent to the client when it connects and removed
        from the buffer. Memory_buffer structure:
        {
            "client_id_1": {
                {CREATED_AT}: 1234567890,
                {MESSAGES}: [str],
            },
        }
        
        Irrespective of the client connected or not, the messages will be deleted after
        {MINUTES_TO_CLEAR_BUFFER} minutes.
        
        Note: we don't need a lock to access this memory buffer because one request is served
        by one client only, and websockets deal with messages from one client sequentially in
        the order they are received.
        """
        self.memory_buffer: Dict[str, Dict[str, Union[int, List[str]]]] = {}

        self.route_to_handler: Dict[str, Callable] = {
            MESSAGES_ENDPOINT: self.messages_handler,
        }

    def start(self):
        server: Serve = websockets.serve(self.main_handler, self.host, self.port)
        asyncio.get_event_loop().run_until_complete(server)
        logger.info(
            message="Websocket server started",
            fields={"server_info": {"host": self.host, "port": self.port}},
        )
        thread = threading.Thread(target=self.auto_clear_memory_buffer, daemon=True)
        thread.start()
        logger.info(
            message="Memory buffer auto-clear thread started",
            fields={"timeout (mins)": MINUTES_TO_CLEAR_BUFFER},
        )
        asyncio.get_event_loop().run_forever()

    async def main_handler(self, websocket: WebSocketServerProtocol, path: str):
        route = path.split("?")[0]
        handler = self.route_to_handler.get(route)
        if handler:
            await handler(websocket, path)
        else:
            logger.error("Invalid path", {"endpoint": path})
            await websocket.close(code=4000, reason="Invalid path")

    async def messages_handler(self, websocket: WebSocketServerProtocol, path: str):
        try:
            query_params = self.get_query_params(path)
        except ValueError as e:
            logger.error("Invalid query params", {"endpoint": path})
            await websocket.send(f"Invalid query params: {repr(e)}")
            return None

        client_id = query_params.get(CLIENT_ID)
        if client_id in self.id_to_client_map:
            logger.warning(
                message=(
                    "Duplicate name: a client with this name is already connected. "
                    "Closing new connection. Provide a new client_id with the request."
                ),
                fields={CLIENT_ID: client_id},
            )
            await websocket.send(
                "Duplicate name: a client with provided client_id already exists."
            )
            return None

        try:
            logger.info("A new client connected", {CLIENT_ID: client_id})
            self.id_to_client_map[client_id] = websocket
            
            # Send all messages in memory buffer to the client
            # Warning: it is important to add client to the `id_to_client_map` before
            # calling `send_buffered_messages` otherwise the sendind to this client will
            # be blocked forever.
            if client_id in self.memory_buffer:
                await self.send_buffered_messages(client_id=client_id)

            await self.send_logic(sender_id=client_id, sender=websocket)
        except ConnectionClosed as e:
            logger.error(
                message="Connection closed with error",
                fields={"Error": e.reason, CLIENT_ID: client_id},
            )
        finally:
            logger.info(
                message="A client just disconnected. Removing from clients list.",
                fields={CLIENT_ID: client_id},
            )
            self.id_to_client_map.pop(client_id, None)

    async def send_logic(self, sender_id: str, sender: WebSocketServerProtocol):
        async for message in sender:
            msg_json: Dict[str, Any]

            try:
                msg_json = json.loads(message)
            except json.JSONDecodeError as e:
                logger.error(
                    message="Expects only JSON string. Invalid JSON data.",
                    fields={"sender_id": sender_id, "sender_data": message},
                )
                continue

            receiver_id = msg_json.pop(RECEIVER_ID, None)
            if not receiver_id:
                logger.error(
                    message=f"No `{RECEIVER_ID}` in message",
                    fields={"sender_id": sender_id, "sender_data": message},
                )
                continue

            logger.info(
                message="Received message from client",
                fields={"sender_id": sender_id, "sender_data": message},
            )

            out_msg_str = json.dumps(msg_json)
            receiver = self.id_to_client_map.get(receiver_id, None)
            if not receiver:
                logger.warning(
                    message=(
                        f"No receiver with this receiver_id. Message stored in memory "
                        "buffer. Messages will be sent when the receiver connects."
                    ),
                    fields={"sender_id": sender_id, RECEIVER_ID: receiver_id},
                )
                self.save_to_memory_buffer(receiver_id=receiver_id, message=out_msg_str)
                continue

            if receiver.closed:
                logger.error(
                    message="Receiver is closed", fields={RECEIVER_ID: receiver_id}
                )
                continue
            
            # Wait while memory buffer is cleared for the receiver
            counter = 0 # to avoid infinite loop
            while receiver_id in self.memory_buffer:
                time.sleep(1)
                if counter == 10:
                    logger.error(
                        message="Memory buffer could not be cleared for receiver",
                        fields={RECEIVER_ID: receiver_id},
                    )
                    break
            
            await receiver.send(out_msg_str)
            logger.info(
                message="Message sent to receiver",
                fields={
                    "sender_id": sender_id,
                    RECEIVER_ID: receiver_id,
                    "message": out_msg_str,
                },
            )

    def get_query_params(self, path: str) -> Dict[str, str]:
        parsed_url = urlparse(path)
        query_parameters = parse_qs(parsed_url.query)

        # Convert single-value lists to single values
        for key, value in query_parameters.items():
            if len(value) == 1:
                query_parameters[key] = value[0]

        if CLIENT_ID not in query_parameters:
            logger.error(
                message="No client_id in query parameters",
                fields={"query_parameters": query_parameters},
            )
            raise ValueError("No client_id in query parameters")

        return query_parameters

    def save_to_memory_buffer(self, receiver_id: str, message: str):
        if receiver_id not in self.memory_buffer:
            self.memory_buffer[receiver_id] = {
                CREATED_AT: int(time.time()),
                MESSAGES: [],
            }
        self.memory_buffer[receiver_id][MESSAGES].append(message)

    async def send_buffered_messages(self, client_id: str):
        all_messages: List[str] = []
        all_messages = self.memory_buffer[client_id][MESSAGES]

        client = self.id_to_client_map.get(client_id, None)
        for message in all_messages:
            await client.send(message)
        
        # Remove the client from memory buffer after sending all messages
        self.memory_buffer.pop(client_id, None)
        logger.info(
            message="Memory buffer cleared",
            fields={CLIENT_ID: client_id},
        )

    def auto_clear_memory_buffer(self):
        for client_id, client_data in self.memory_buffer.items():
            if (
                int(time.time()) - client_data[CREATED_AT]
                > MINUTES_TO_CLEAR_BUFFER * 60
                and client_id not in self.id_to_client_map
            ):
                self.memory_buffer.pop(client_id, None)
                logger.info(
                    message="Memory buffer cleared after timeout",
                    fields={
                        CLIENT_ID: client_id,
                        "timeout (mins)": MINUTES_TO_CLEAR_BUFFER,
                    },
                )
            # sleep time added to avoid resource overloading
            time.sleep(20)


def start_ws_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
    ws_server = WebSocketServer(host=host, port=port)
    process = multiprocessing.Process(target=ws_server.start, daemon=True)
    process.start()


if __name__ == "__main__":
    start_ws_server()

