from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.__stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"stream_id": self.__stream_id}


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.__type = "Environmental Data"
        self.__readings = 0
        self.__critical = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        readings = 0
        temp_sum = 0
        temp_readings = 0
        for data in data_batch:
            for item in data.items():
                readings += 1
                self.__readings += 1
                key, value = item
                if key == "temp":
                    temp_sum += value
                    temp_readings += 1
                    if value >= 30 or value <= 5:
                        self.__critical += 1
                elif (key == "humidity" and
                    (value >= 80 or value <= 20)):
                        self.__critical += 1
                elif (key == "pressure" and
                   (value <= 950 or value >= 1050)):
                        self.__critical += 1
        try:
            temp_average = temp_sum / temp_readings
        except ZeroDivisionError:
            return f"{readings} readings processed"
        return f"{readings} readings processed, avg temp: {temp_average:.1f}ºC"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return super().filter_data(data_batch, criteria)
        data = []
        for batch in data_batch:
            if isinstance(batch, dict):
                for item in batch.items():
                    key, value = item
                    if isinstance(key, str) and isinstance(value, int):
                        if "high-priority" not in criteria:
                            if (key == "temp" or key == "humidity" or
                                key == "pressure"):
                                data.append({key: value})
                        else:
                            if (key == "temp" and
                            (value >= 30 or value <= 5)):
                                data.append({key: value})
                            elif (key == "humidity" and
                            (value >= 80 or value <= 20)):
                                data.append({key: value})
                            elif (key == "pressure" and
                            (value <= 950 or value >= 1050)):
                                data.append({key: value})
        return data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.__stream_id,
            "type": self.__type,
            "readings": self.__readings,
            "critical": self.__critical
        }


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.__type = "Financial Data"
        self.__operations = 0
        self.__large = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        operations = 0
        net_flow = 0
        for data in data_batch:
            for item in data.items():
                operations += 1
                self.__operations += 1
                key, value = item
                if key == "buy":
                    net_flow += value
                elif key == "sell":
                    net_flow -= value
                if value >= 500:
                    self.__large += 1
        if net_flow >= 0:
            return f"{operations} operations, net flow +{net_flow}"
        return f"{operations} operations, net flow {net_flow}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return super().filter_data(data_batch, criteria)
        data = []
        for batch in data_batch:
            if isinstance(batch, dict):
                for item in batch.items():
                    key, value = item
                    if isinstance(key, str) and isinstance(value, int):
                        if "high-priority" not in criteria:
                            if key == "buy" or key == "sell":
                                data.append({key: value})
                        else:
                            if key == "buy" and value >= 500:
                                data.append({key: value})
                            elif (key == "sell" and value >= 500):
                                data.append({key: value})
        return data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.__stream_id,
            "type": self.__type,
            "operations": self.__operations,
            "large": self.__large
        }


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        pass

    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return super().filter_data(data_batch, criteria)
        data = []
        for item in data_batch:
            if isinstance(item, str):
                if "high-priority" not in criteria:
                    if (item == "login" or item == "logout" or
                        item == "error" or item == "failure"):
                        data.append(item)
                else:
                    if item == "failure":
                        data.append(item)
        return data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class StreamProcessor():
    pass


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()

"""
Initializing Sensor Stream...
Stream ID: SENSOR_001, Type: Environmental Data
Processing sensor batch: [temp:22.5, humidity:65, pressure:1013]
Sensor analysis: 3 readings processed, avg temp: 22.5°C
Initializing Transaction Stream...
Stream ID: TRANS_001, Type: Financial Data
Processing transaction batch: [buy:100, sell:150, buy:75]
Transaction analysis: 3 operations, net flow: +25 units
Initializing Event Stream...
Stream ID: EVENT_001, Type: System Events
Processing event batch: [login, error, logout]
Event analysis: 3 events, 1 error detected
=== Polymorphic Stream Processing ===
Processing mixed stream types through unified interface...
Batch 1 Results:
- Sensor data: 2 readings processed
- Transaction data: 4 operations processed
- Event data: 3 events processed
Stream filtering active: High-priority data only
Filtered results: 2 critical sensor alerts, 1 large transaction
All streams processed successfully. Nexus throughput optimal.
"""