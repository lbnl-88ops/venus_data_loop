import asyncio
import threading
from unittest.mock import patch, MagicMock, AsyncMock, ANY

import pytest

from ops.ecris.model.measurement import CurrentMeasurement
from venus_current_loop import venus_data_loop

# The full path to the modules AS THEY ARE IMPORTED in venus_current_loop.py
MODULE_PATH = 'venus_current_loop.'

@pytest.mark.asyncio
async def test_venus_data_loop_integration():
    with patch(MODULE_PATH + 'Ammeter', autospec=True) as MockAmmeter, \
         patch(MODULE_PATH + 'VenusPLC', autospec=True) as MockVenusPLC, \
         patch(MODULE_PATH + 'threading.Thread', autospec=True) as MockThread, \
         patch(MODULE_PATH + 'update_plc_average_current', autospec=True) as mock_update_plc:

        mock_ammeter_instance = MockAmmeter.return_value
        mock_plc_instance = MockVenusPLC.return_value

        loop_task = asyncio.create_task(venus_data_loop("dummy_ip", 1234))

        await asyncio.sleep(0.01)

        MockThread.assert_called_once()
        thread_args = MockThread.call_args.kwargs['args']
        the_queue = thread_args[1]

        test_measurement = CurrentMeasurement(
            source='test_ammeter', 
            average=1.23e-6, 
            standard_deviation=0.05
        )

        await the_queue.put(test_measurement)

        await asyncio.sleep(0.01)

        mock_update_plc.assert_awaited_once_with(mock_plc_instance, test_measurement)
        
        mock_ammeter_instance.connect.assert_awaited_once()
        mock_ammeter_instance.reset.assert_awaited_once()

        loop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop_task
        mock_ammeter_instance.disconnect.assert_awaited_once()