# @Author: yufeiz
# @Date:   2018-11-02T17:46:01-07:00
# @Last modified by:   yufeiz
# @Last modified time: 2018-11-02T17:50:08-07:00



#!/usr/bin/env python

# WS server example

import asyncio
import websockets

async def hello(websocket, path):
    name = await websocket.recv()
    print(f"< {name}")

    greeting = f"Hello {name}!"

    await websocket.send(greeting)
    print(f"> {greeting}")

start_server = websockets.serve(hello, 'localhost', 8765)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
