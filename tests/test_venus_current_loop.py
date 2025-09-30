import asyncio
from unittest.mock import patch, PropertyMock
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from ops.ecris.model.measurement import CurrentMeasurement
from venus_current_loop import venus_data_loop

MODULE_PATH = 'venus_current_loop.'

@dataclass
class LoopMocks:
    MockAmmeter: MagicMock
    MockVenusPLC: MagicMock
    mock_ammeter_instance: MagicMock
    mock_plc_instance: MagicMock

@pytest.fixture
def mocked_loop_dependencies():
    with patch(MODULE_PATH + 'Ammeter', autospec=True) as MockAmmeter, \
         patch(MODULE_PATH + 'VenusPLC', autospec=True) as MockVenusPLC:

        mocks = LoopMocks(
            MockAmmeter=MockAmmeter,
            MockVenusPLC=MockVenusPLC,
            mock_ammeter_instance=MockAmmeter.return_value,
            mock_plc_instance=MockVenusPLC.return_value
        )
        yield mocks

@pytest.mark.asyncio
async def test_venus_data_loop_integration(mocked_loop_dependencies):
    mocks = mocked_loop_dependencies
    with patch(MODULE_PATH + 'threading.Thread', autospec=True) as MockThread, \
         patch(MODULE_PATH + 'update_plc_average_current', autospec=True) as mock_update_plc:

        type(mocks.mock_ammeter_instance).is_connected = PropertyMock(return_value=True)

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

        mock_update_plc.assert_awaited_once_with(mocks.mock_plc_instance, test_measurement)
        
        mocks.mock_ammeter_instance.connect.assert_awaited_once()
        mocks.mock_ammeter_instance.reset.assert_awaited_once()

        loop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop_task
        mocks.mock_ammeter_instance.disconnect.assert_awaited_once()

@pytest.mark.asyncio
async def test_venus_data_loop_connection_error(mocked_loop_dependencies):
    mocks = mocked_loop_dependencies

    mocks.mock_ammeter_instance.connect.side_effect = ConnectionError()
    type(mocks.mock_ammeter_instance).is_connected = PropertyMock(return_value=False)

    with pytest.raises(ConnectionError):
        await venus_data_loop("dummy_ip", 1234)
    
    mocks.mock_ammeter_instance.connect.assert_awaited_once()
    assert mocks.mock_ammeter_instance.disconnect.call_count == 0