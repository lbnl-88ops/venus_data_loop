import threading
from functools import partial
import asyncio
import logging
from argparse import ArgumentParser

#from venus_data_project import VenusController
from ops.ecris.devices.venus_plc import VENUSController

from ops.ecris.devices import Ammeter, VenusPLC
from ops.ecris.operations.producers import time_average_current
from ops.ecris.data.producer_thread import producer_thread
from ops.ecris.tasks.device_broadcasters import update_plc_average_current
from ops.ecris.model.measurement import CurrentMeasurement
from ops.ecris.tasks.websocket_broadcaster import WebSocketBroadcaster

_log = logging.getLogger('ops')

async def consumer(queue: asyncio.Queue, venus_plc: VenusPLC, websocket: WebSocketBroadcaster):
    logging.info("Broadcaster started, waiting for data...")
    while True:
        measurement: CurrentMeasurement = await queue.get()
        logging.debug(f"Received new measurement from {measurement.source}: Avg={measurement.average:.4e}, std={measurement.standard_deviation}")
        tasks = [update_plc_average_current(venus_plc, measurement), websocket.broadcast(measurement)]
        await asyncio.gather(*tasks)
        queue.task_done()

async def venus_data_loop(ammeter_ip: str, ammeter_port: int):
    _log.info('Starting VENUS data loop')
    loop = asyncio.get_running_loop()
    interval = 0.33
    consumer_task: asyncio.Task | None = None
    current_queue = asyncio.Queue()
    ammeter = Ammeter(read_frequency_per_min=1000, ip=ammeter_ip, prompt='B2900A>',
                      port=ammeter_port, id='KeySight B2900A Faraday Cup')
    venus_plc = VenusPLC(VENUSController(read_only=False))    
    current_measurement_producer = partial(time_average_current, loop=loop, 
                                           ammeter=ammeter, average_seconds = interval)
    broadcaster = WebSocketBroadcaster('127.0.0.1', 8765)

    thread = threading.Thread(
        target=producer_thread,
        args=(loop, current_queue, current_measurement_producer, interval),
        daemon=True)

    try:
        _log.info('Setting up websocket...')
        await broadcaster.start()
        _log.debug('Websocket set up...')
        _log.info('Connecting to ammeter...')
        await ammeter.connect()
        _log.debug('Ammeter connected.')
        await ammeter.reset()
        _log.info('Ammeter connected and reset.')
        thread.start()
        _log.info('Current producer thread started.')

        consumer_task = asyncio.create_task(
            consumer(current_queue, venus_plc, broadcaster)
        )        

        await asyncio.Future()
    
    except (KeyboardInterrupt, asyncio.CancelledError):
        logging.info("Shutdown signal received, shutting down...")
        raise
    except Exception:
        logging.info("An unknown exception occured, shutting down...")
        raise
    finally:
        logging.info("Cleaning up resources...")
        await broadcaster.stop()
        if consumer_task:
            consumer_task.cancel()
            await asyncio.gather(consumer_task, return_exceptions=True)
        if ammeter.is_connected:
            logging.debug("Disconnecting Ammeter...")
            await ammeter.disconnect()
            logging.info("Ammeter disconnected. Exiting.")


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-d", "--debug", action="store_true", help="run with debug logging")
    parser.add_argument("ip", help="Ammeter IP address")
    parser.add_argument("port", help="Ammeter port", type=int)
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    try: 
        asyncio.run(venus_data_loop(args.ip, args.port))
    except KeyboardInterrupt:
        print('Program terminated by user.')
