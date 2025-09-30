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

_log = logging.getLogger('ops')

async def plc_updater_consumer(queue: asyncio.Queue, venus_plc: VenusPLC):
    logging.info("VENUSPLC Updater started, waiting for data...")
    while True:
        measurement: CurrentMeasurement = await queue.get()
        logging.debug(f"Received new measurement from {measurement.source}: Avg={measurement.average:.4e}")
        await update_plc_average_current(venus_plc, measurement)
        queue.task_done()

async def venus_data_loop(ammeter_ip: str, ammeter_port: int):
    _log.info('Starting VENUS data loop')
    loop = asyncio.get_running_loop()
    interval = 0.33
    consumer_task: asyncio.Task | None = None
    current_queue = asyncio.Queue()
    ammeter = Ammeter(read_frequency_per_min=1000, ip=ammeter_ip, 
                      port=ammeter_port, id='KeySight B2900A Faraday Cup')
    venus_plc = VenusPLC(VENUSController(read_only=False))    
    current_measurement_producer = partial(time_average_current, loop=loop, 
                                           ammeter=ammeter, average_seconds = interval)
    thread = threading.Thread(
        target=producer_thread,
        args=(loop, current_queue, current_measurement_producer, interval),
        daemon=True)

    try:
        _log.info('Connecting to ammeter...')
        await ammeter.connect()
        _log.debug('Ammeter connected.')
        await ammeter.reset()
        _log.info('Ammeter connected and reset.')
        thread.start()
        _log.info('Current producer thread started.')

        consumer_task = asyncio.create_task(
            plc_updater_consumer(current_queue, venus_plc)
        )        

        await asyncio.Future()
    
    except (KeyboardInterrupt, asyncio.CancelledError):
        logging.info("Shutdown signal received.")
    finally:
        logging.info("Cleaning up resources...")
        if consumer_task:
            consumer_task.cancel()
            await asyncio.gather(consumer_task, return_exceptions=True)
        
        logging.debug("Disconnecting Ammeter...")
        await ammeter.disconnect()
        logging.info("Ammeter disconnected. Exiting.")
        raise 


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-d", "--debug", default=False, help="run with debug logging")
    parser.add_argument("ip", help="Ammeter IP address")
    parser.add_argument("port", help="Ammeter port", type=int)
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    try: 
        asyncio.run(venus_data_loop(args.ip, args.port))
    except KeyboardInterrupt:
        print('Program terminated by user.')
