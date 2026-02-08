from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"stream_id": self.stream_id}


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.type = "Environmental Data"
        self.readings = 0
        self.critical = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        readings = 0
        temp_sum = 0
        temp_readings = 0
        for data in data_batch:
            for item in data.items():
                readings += 1
                self.readings += 1
                key, value = item
                if key == "temp":
                    temp_sum += value
                    temp_readings += 1
                    if value >= 30 or value <= 5:
                        self.critical += 1
                elif (key == "humidity"
                      and (value >= 80 or value <= 20)):
                    self.critical += 1
                elif (key == "pressure"
                      and (value <= 950 or value >= 1050)):
                    self.critical += 1
        try:
            temp_average = temp_sum / temp_readings
        except ZeroDivisionError:
            return f"{readings} readings processed"
        return f"{readings} readings processed, avg temp: {temp_average:.1f}ºC"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        data = []
        for batch in data_batch:
            if isinstance(batch, dict):
                for item in batch.items():
                    key, value = item
                    if (isinstance(key, str)
                            and isinstance(value, (int, float))):
                        if criteria is None:
                            if (key == "temp" or key == "humidity"
                                    or key == "pressure"):
                                data.append({key: value})
                        elif "high-priority" in criteria:
                            if (key == "temp"
                                    and (value >= 30 or value <= 5)):
                                data.append({key: value})
                            elif (key == "humidity"
                                    and (value >= 80 or value <= 20)):
                                data.append({key: value})
                            elif (key == "pressure"
                                    and (value <= 950 or value >= 1050)):
                                data.append({key: value})
        return data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": self.type,
            "readings": self.readings,
            "critical": self.critical
        }


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.type = "Financial Data"
        self.operations = 0
        self.large = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        operations = 0
        net_flow = 0
        for data in data_batch:
            for item in data.items():
                operations += 1
                self.operations += 1
                key, value = item
                if key == "buy":
                    net_flow += value
                elif key == "sell":
                    net_flow -= value
                if value >= 500:
                    self.large += 1
        if net_flow >= 0:
            return f"{operations} operations, net flow +{net_flow} units"
        return f"{operations} operations, net flow {net_flow} units"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        data = []
        for batch in data_batch:
            if isinstance(batch, dict):
                for item in batch.items():
                    key, value = item
                    if isinstance(key, str) and isinstance(value, int):
                        if criteria is None:
                            if key == "buy" or key == "sell":
                                data.append({key: value})
                        elif "high-priority" in criteria:
                            if key == "buy" and value >= 500:
                                data.append({key: value})
                            elif (key == "sell" and value >= 500):
                                data.append({key: value})
        return data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": self.type,
            "operations": self.operations,
            "large": self.large
        }


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.type = "System Events"
        self.events = 0
        self.failures = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        events = 0
        errors = 0
        for event in data_batch:
            events += 1
            self.events += 1
            if event == "error":
                errors += 1
            elif event == "failure":
                self.failures += 1
        if errors == 1:
            return f"{events} events, {errors} error detected"
        return f"{events} events, {errors} errors detected"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        data = []
        for item in data_batch:
            if isinstance(item, str):
                if criteria is None:
                    if (item == "login" or item == "logout"
                            or item == "error" or item == "failure"):
                        data.append(item)
                elif "high-priority" in criteria:
                    if item == "failure":
                        data.append(item)
        return data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": self.type,
            "events": self.events,
            "failures": self.failures
        }


class StreamProcessor():
    def __init__(self, s_stream: str, t_stream: str, e_stream: str) -> None:
        self.streams = [
            SensorStream(s_stream),
            TransactionStream(t_stream),
            EventStream(e_stream)
        ]

    def process_batch(self, data_batch: List[Any],
                      criteria: Optional[str] = None) -> str:
        index = 0
        processed = ""
        while index < len(self.streams):
            data = self.streams[index].filter_data(data_batch, criteria)
            if index == 0:
                processed += f"- Sensor data: {len(data)} readings processed\n"
            elif index == 1:
                processed += ("- Transaction data: "
                              f"{len(data)} operations processed\n")
            elif index == 2:
                processed += f"- Event data: {len(data)} events processed"
            index += 1
        return processed

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> str:
        result_s = ""
        result_t = ""
        result_e = ""
        result = ""
        index = 0
        while index < len(self.streams):
            filtered = self.streams[index].filter_data(data_batch, criteria)
            if index == 0 and len(filtered) > 0:
                if criteria is None:
                    if len(filtered) == 1:
                        result_s += f"{len(filtered)} sensor alert"
                    else:
                        result_s += f"{len(filtered)} sensor alerts"
                elif "high-priority" in criteria:
                    if len(filtered) == 1:
                        result_s += f"{len(filtered)} critical sensor alert"
                    else:
                        result_s += f"{len(filtered)} critical sensor alerts"
            elif index == 1 and len(filtered) > 0:
                if criteria is None:
                    if len(filtered) == 1:
                        result_t += f"{len(filtered)} transaction"
                    else:
                        result_t += f"{len(filtered)} transactions"
                elif "high-priority" in criteria:
                    if len(filtered) == 1:
                        result_t += f"{len(filtered)} large transaction"
                    else:
                        result_t += f"{len(filtered)} large transactions"
            elif index == 2 and len(filtered) > 0:
                if criteria is None:
                    if len(filtered) == 1:
                        result_e += f"{len(filtered)} event"
                    else:
                        result_e += f"{len(filtered)} events"
                elif "high-priority" in criteria:
                    if len(filtered) == 1:
                        result_e += f"{len(filtered)} failure event"
                    else:
                        result_e += f"{len(filtered)} failure events"
            index += 1
        if len(result_s) > 0:
            result += result_s
            if len(result_t) > 0 or len(result_e) > 0:
                result += ", "
        if len(result_t) > 0:
            result += result_t
            if len(result_e) > 0:
                result += ", "
        if len(result_e) > 0:
            result += result_e
        return result


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()
    print("Initializing Sensor Stream...")
    s_stream = SensorStream("SENSOR_001")
    s_data = [
        {"temp": 22.5},
        {"humidity": 65},
        {"pressure": 1013}
    ]
    s_stream_stats = s_stream.get_stats()
    print(f"Stream ID: {s_stream_stats['stream_id']}, "
          f"Type: {s_stream_stats['type']}")
    print(f"Processing sensor batch: {s_data}")
    print(f"Sensor analysis: {s_stream.process_batch(s_data)}")
    print()
    print("Initializing Transaction Stream...")
    t_stream = TransactionStream("TRANS_001")
    t_data = [
        {"buy": 100},
        {"sell": 150},
        {"buy": 75}
    ]
    t_stream_stats = t_stream.get_stats()
    print(f"Stream ID: {t_stream_stats['stream_id']}, "
          f"Type: {t_stream_stats['type']}")
    print(f"Processing transaction batch: {t_data}")
    print(f"Transaction analysis: {t_stream.process_batch(t_data)}")
    print()
    print("Initializing Event Stream...")
    e_stream = EventStream("EVENT_001")
    e_data = ["login", "error", "logout"]
    e_stream_stats = e_stream.get_stats()
    print(f"Stream ID: {e_stream_stats['stream_id']}, "
          f"Type: {e_stream_stats['type']}")
    print(f"Processing event batch: {e_data}")
    print(f"Event analysis: {e_stream.process_batch(e_data)}")
    print()
    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    p_stream = StreamProcessor("SENSOR_002", "TRANS_002", "EVENT_002")
    p_data = [
        {"temp": 35, "humidity": 99},
        {"buy": 100, "sell": 150},
        {"buy": 75, "sell": 600},
        "login", "error", "logout"
    ]
    print("")
    print("Batch 1 Results:")
    print(p_stream.process_batch(p_data))
    print()
    print("Stream filtering active: High-priority data only")
    print("Filtered results: "
          f"{p_stream.filter_data(p_data, 'high-priority')}")
    print()
    print("All streams processed successfully. Nexus throughput optimal.")
