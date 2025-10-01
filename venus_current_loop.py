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
from ops.ecris.services.data_aquisition import TelnetDataAquisition

_log = logging.getLogger('ops')

async def consumer(queue: asyncio.Queue, venus_plc: VenusPLC, websocket: WebSocketBroadcaster):
    _log.info("Consumer task started, waiting for data...")
    while True:
        measurement: CurrentMeasurement = await queue.get()
        _log.debug(f"Received new measurement from {measurement.source}: Avg={measurement.average:.4e}, std={measurement.standard_deviation:.2f}%")
        tasks = [update_plc_average_current(venus_plc, measurement), websocket.broadcast(measurement)]
        await asyncio.gather(*tasks)
        queue.task_done()

async def venus_data_loop(ammeter_ip: str, ammeter_port: int):
    _log.info('Starting VENUS data loop')
    interval = 0.33
    consumer_task: asyncio.Task | None = None
    # Devices
    ammeter = Ammeter(read_frequency_per_min=1000, ip=ammeter_ip, prompt='B2900A>',
                      port=ammeter_port, id='KeySight B2900A Faraday Cup')
    venus_plc = VenusPLC(VENUSController(read_only=False))    
    
    # Services
    aquisition_service = TelnetDataAquisition(ammeter)
    broadcaster = WebSocketBroadcaster('127.0.0.1', 8765)

    current_measurement_aquisition = partial(time_average_current, average_seconds=interval)

    try:
        _log.info('Starting services...')
        await broadcaster.start()
        await aquisition_service.start(current_measurement_aquisition, interval)
        _log.info('All services running.')

        consumer_task = asyncio.create_task(consumer(aquisition_service.data_queue, venus_plc, broadcaster))        

        _log.info("Application is running. Press Ctrl+C to exit.")
        await asyncio.Future()
    
    except (KeyboardInterrupt, asyncio.CancelledError):
        _log.info("Shutdown signal received...")
        raise
    except Exception:
        _log.info("An unknown exception occured, shutting down...")
        raise
    finally:
        _log.info("Cleaning up resources...")
        if consumer_task:
            consumer_task.cancel()
            await asyncio.gather(consumer_task, return_exceptions=True)
        
        await aquisition_service.stop()
        await broadcaster.stop()
        
        _log.info("Cleanup complete. Exiting.")


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
