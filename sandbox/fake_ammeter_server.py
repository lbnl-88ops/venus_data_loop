import asyncio
import logging
import math
import itertools
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - SERVER - %(levelname)s - %(message)s')

PROMPT = 'B2900A>'

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    client_addr = writer.get_extra_info('peername')
    logging.info(f"Client connected from {client_addr}")
    
    step_counter = itertools.count()

    try:
        writer.write(f'{PROMPT}\r\n'.encode('ascii'))
        await writer.drain()

        while True:
            line = await reader.readline()
            if not line:
                logging.info("Client disconnected.")
                break

            command = line.decode().strip()
            logging.info(f"Received command: '{command}'")

            await asyncio.sleep(0.05)
            response = None

            if 'meas:curr?' in command: 
                current_value = 100 + 50 * math.sin(next(step_counter) * 0.1) + 5*random.gauss(0, 10)
                response = f'{current_value:+E}\r\n'.encode('ascii') 

            logging.info("Sending echo")
            writer.write(f'{PROMPT} {command}\r\n'.encode('ascii'))
            if response:
                logging.info(f"Sending data: {response.decode().strip()}")
                writer.write(response)

            await writer.drain()

    except ConnectionResetError:
        logging.warning("Client connection reset.")
    finally:
        logging.info(f"Closing connection for {client_addr}")
        try:
            writer.close()
            await writer.wait_closed()
        except BrokenPipeError:
            logging.info(f"Connection for {client_addr} was already broken. Closed forcefully.")
            # We don't need to do anything else, the connection is already gone.
            pass



async def main():
    """Starts the TCP server."""
    host = 'localhost'
    port = 12345 # An arbitrary free port
    
    server = await asyncio.start_server(handle_client, host, port)
    logging.info(f"Fake Ammeter Server running on {host}:{port}. Waiting for connections...")

    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer shut down by user.")