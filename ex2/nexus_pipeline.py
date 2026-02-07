from typing import Any, List, Dict, Union, Optional, Protocol
from abc import ABC, abstractmethod


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        return data


class InputStage:
    def process(self, data: Any) -> Any:
        if isinstance(data, dict):
            processed = {}
            if "sensor" in data:
                if (data.get("sensor") == "temp" or
                    data.get("sensor") == "humidity" or
                    data.get("sensor") == "pressure"):
                    processed.update({"sensor": data.get("sensor")})
                else:
                    return {}
                if "value" in data:
                    processed.update({"value": float(data.get("value"))})
                else:
                    return {}
                if "unit" in data:
                    processed.update({"unit": data.get("unit")})
                else:
                    return {}
            return processed
        if isinstance(data, str) and "," in data:
            processed = {}
            data_split = data.split(",")
            index = 1
            total_actions = 0
            if len(data_split) % 3 != 0:
                return {}
            while index < len(data_split):
                    total_actions += 1
                    index += 3
            processed.update({"actions": total_actions})
            return processed
        if isinstance(data, list):
            processed = {}
            total = 0
            index = 0
            while index < len(data):
                total += float(data[index])
                index += 1
            processed.update({"readings": len(data)})
            processed.update({"average": (total / len(data))})
            return processed
        else:
            return {}


class TransformStage:
    def process(self, data: Any) -> Any:
        if not data :
            return None
        transformed = {}
        if "sensor" in data:
            transformed.update({"type": "sensor"})
            if data.get("sensor") == "temp":
                transformed.update({"sensor": "temperature"})
                transformed.update({"value": data["value"]})
                if data["value"] <= 5 or data["value"] >= 30:
                     transformed.update({"range": "Critical"})
                else:
                     transformed.update({"range": "Normal"})
                transformed.update({"unit": data["unit"]})
            elif data.get("sensor") == "humidity":
                transformed.update({"sensor": "humidity"})
                transformed.update({"value": data["value"]})
                if data["value"] <= 20 or data["value"] >= 80:
                     transformed.update({"range": "Critical"})
                else:
                     transformed.update({"range": "Normal"})
                transformed.update({"unit": data["unit"]})
            else:
                transformed.update({"sensor": "pressure"})
                transformed.update({"value": data["value"]})
                if data["value"] <= 950 or data["value"] >= 1050:
                     transformed.update({"range": "Critical"})
                else:
                     transformed.update({"range": "Normal"})
                transformed.update({"unit": data["unit"]})
            return transformed
        if "actions" in data:
            transformed.update({"type": "user"})
            transformed.update({"actions": data["actions"]})
            return transformed
        if "readings" in data:
            transformed.update({"type": "stream"})
            transformed.update({"readings": data["readings"]})
            transformed.update({"average": data["average"]})
            return transformed
        return None


class OutputStage:
    def process(self, data: Any) -> Any:
        if data is None or not data:
            return ""
        if data["type"] == "sensor":
            sensor_type = data["sensor"]
            value = data["value"]
            sensor_range = data["range"]
            unit = data["unit"]
            if sensor_type == "temperature":
                return (f"Processed {sensor_type} reading: {value}º{unit} "
                    f"({sensor_range} range)")
            return (f"Processed {sensor_type} reading: {value}{unit} "
                    f"({sensor_range} range)")
        if data["type"] == "user":
            actions = data["actions"]
            return f"User activity logged: {actions} actions processed"
        if data["type"] == "stream":
            readings = data["readings"]
            average = data["average"]
            return (f"Stream summary: {readings} readings, avg: "
                    f"{average:.1f}ºC")


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id: str):
        self.stages : List[ProcessingStage] = []
        self.pipeline_id = pipeline_id

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        pass

    def add_stage(self, stage: ProcessingStage) -> None:
        if stage is not None:
            self.stages.append(stage)


class JSONAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        current_data = data
        for stage in self.stages:
            current_data = stage.process(current_data)
        return current_data


class CSVAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        current_data = data
        for stage in self.stages:
            current_data = stage.process(current_data)
        return current_data


class StreamAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        current_data = data
        for stage in self.stages:
            current_data = stage.process(current_data)
        return current_data


class NexusManager():
    pass


if __name__ == "__main__":
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print()
    print("Initializing Nexus Manager...")
    #Initialize here
    print("Pipeline capacity: 1000 streams/second")
    print()
    print("Creating Data Processing Pipeline...")
    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery")
    print()
    print("=== Multi-Format Data Processing ===")
    print()
    print("Processing JSON data through pipeline...")


"""

Input: {"sensor": "temp", "value": 23.5, "unit": "C"}
Transform: Enriched with metadata and validation
Output: Processed temperature reading: 23.5°C (Normal range)
Processing CSV data through same pipeline...
Input: "user,action,timestamp"
Transform: Parsed and structured data
Output: User activity logged: 1 actions processed
Processing Stream data through same pipeline...
Input: Real-time sensor stream
Transform: Aggregated and filtered
Output: Stream summary: 5 readings, avg: 22.1°C
=== Pipeline Chaining Demo ===
Pipeline A -> Pipeline B -> Pipeline C
Data flow: Raw -> Processed -> Analyzed -> Stored
Chain result: 100 records processed through 3-stage pipeline
Performance: 95% efficiency, 0.2s total processing time
=== Error Recovery Test ===
Simulating pipeline failure...
Error detected in Stage 2: Invalid data format
Recovery initiated: Switching to backup processor
Recovery successful: Pipeline restored, processing resumed
Nexus Integration complete. All systems operational.
"""