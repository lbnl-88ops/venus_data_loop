import asyncio
import logging
from argparse import ArgumentParser
import json

#from venus_data_project import VenusController
from ops.ecris.devices.venus_plc import VENUSController
from ops.ecris.model.measurement import Measurement, MultiValueMeasurement
from ops.ecris.devices import Ammeter, VenusPLC
from ops.ecris.tasks.device_broadcasters import update_plc_average_current
from ops.ecris.services import WebSocketBroadcaster
from ops.ecris.services.ammeter import CurrentAcquisitionService, AverageCurrentService
from ops.ecris.services.venus_plc import PLCDataAquisitionService

_log = logging.getLogger('ops')

async def broadcast_venus_data(queue: asyncio.Queue, websocket: WebSocketBroadcaster):
    while True:
        data: MultiValueMeasurement = await queue.get()
        output_data = data.values
        output_data['timestamp'] = data.timestamp
        await websocket.broadcast(json.dumps(output_data))
        queue.task_done()


async def venus_data_loop():
    _log.info('Starting VENUS PLC data loop')

    venus_plc = VenusPLC(VENUSController(read_only=False))
    
    venus_data_service = PLCDataAquisitionService(venus_plc)

    venus_ws = WebSocketBroadcaster('127.0.0.1', 8767)


    try:
        _log.info('Starting services...')
        # Initial services
        await asyncio.gather(
            venus_ws.start(),
        )
        # Data collection services 
        await venus_data_service.start()

        _log.info('All services running.')

        # Processing services
        tasks = [
            broadcast_venus_data(venus_data_service.data_queue, venus_ws)
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
            venus_ws.stop(),
            return_exceptions=True
        )
        _log.info("Cleanup complete. Exiting.")


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-d", "--debug", action="store_true", help="run with debug logging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    try: 
        asyncio.run(venus_data_loop())
    except KeyboardInterrupt:
        print('Program terminated by user.')
