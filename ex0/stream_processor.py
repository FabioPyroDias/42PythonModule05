from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data : Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        if isinstance(data, (int)):
            return f"Processed 1 numeric value, sum={data}, avg={data}"
        return f"Processed {len(data)} numeric values, sum={sum(data)}, avg={sum(data) / len(data):.1f}"

    def validate(self, data: Any) -> bool:
        if isinstance(data, (list)):
            for number in data:
                if not isinstance(number, (int)):
                    return False
            return True
        elif isinstance(data, (int)):
            return True
        else:
            return False

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class TextProcessor(DataProcessor):
    pass


class LogProcessor(DataProcessor):
    pass


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()
    print("Initializing Numeric Processor...")
    processor_n = NumericProcessor()
    data_np = [1, 2, 3, 4, 5]
    print(f"Processing data: {data_np}")
    if processor_n.validate(data_np):
        print("Validation: Numeric data verified")
        print(processor_n.format_output(processor_n.process(data_np)))
    else:
        print("Validation: Invalid input")
    print()
    print("Initializing Text Processor...")
    processor_t = TextProcessor()
    data_tp = "Hello Nexus World"
    print(f"Processing data: \"{data_tp}\"")
"""

Processing data: "Hello Nexus World"
Validation: Text data verified
Output: Processed text: 17 characters, 3 words
Initializing Log Processor...
Processing data: "ERROR: Connection timeout"
Validation: Log entry verified
Output: [ALERT] ERROR level detected: Connection timeout
=== Polymorphic Processing Demo === """