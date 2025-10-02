import asyncio
import logging
from argparse import ArgumentParser

#from venus_data_project import VenusController
from ops.ecris.devices.venus_plc import VENUSController
from ops.ecris.model.measurement import Measurement
from ops.ecris.devices import Ammeter, VenusPLC
from ops.ecris.tasks.device_broadcasters import update_plc_average_current
from ops.ecris.services import WebSocketBroadcaster
from ops.ecris.services.ammeter import CurrentAcquisitionService, AverageCurrentService

_log = logging.getLogger('ops')

async def broadcaster_consumer(queue: asyncio.Queue, websocket: WebSocketBroadcaster):
    """Generic consumer that broadcasts any measurement."""
    while True:
        measurement: Measurement = await queue.get()
        await websocket.broadcast(measurement)
        queue.task_done()

async def average_consumer(queue: asyncio.Queue, venus_plc: VenusPLC, websocket: WebSocketBroadcaster):
    """Specialized consumer for averaged data."""
    _log.info("Average consumer started, waiting for data...")
    while True:
        measurement: Measurement = await queue.get()
        tasks = [
            venus_plc.write_data(VenusPLC.DataKeys.AVERAGE_CURRENT, measurement.average),
            venus_plc.write_data(VenusPLC.DataKeys.CURRENT_STDEV, measurement.standard_deviation),
            websocket.broadcast(measurement)
        ]
        await asyncio.gather(*tasks)
        queue.task_done()

async def venus_data_loop(ammeter_ip: str, ammeter_port: int):
    _log.info('Starting VENUS data loop')
    average_interval = 0.33
    
    ammeter = Ammeter(read_frequency_per_min=1000, ip=ammeter_ip, prompt='B2900A>',
                      port=ammeter_port, id='KeySight B2900A Faraday Cup')
    venus_plc = VenusPLC(VENUSController(read_only=False))
    
    current_service = CurrentAcquisitionService(ammeter)
    average_service = AverageCurrentService(current_service, average_interval)

    avg_ws = WebSocketBroadcaster('127.0.0.1', 8765)
    raw_ws = WebSocketBroadcaster('127.0.0.1', 8766)

    raw_data_input_queue = current_service.distributor.subscribe()
    averaged_data_input_queue = average_service.data_queue

    try:
        _log.info('Starting services...')
        # Initial services
        await asyncio.gather(
            raw_ws.start(),
            avg_ws.start()
        )
        # Data collection services 
        await current_service.start()
        
        _log.info('All services running.')

        # Processing services
        tasks = [
            average_service.start(), 
            average_consumer(averaged_data_input_queue, venus_plc, avg_ws),
            broadcaster_consumer(raw_data_input_queue, raw_ws)
        ]
        _log.info("Application is running. Press Ctrl+C to exit.")
        await asyncio.gather(*tasks)
    
    except (KeyboardInterrupt, asyncio.CancelledError):
        _log.info("Shutdown signal received...")
    except Exception:
        _log.exception("An unknown exception occurred, shutting down...")
    finally:
        _log.info("Cleaning up resources...")
        # Stop everything gracefully
        await asyncio.gather(
            current_service.stop(),
            raw_ws.stop(),
            avg_ws.stop(),
            return_exceptions=True
        )
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
