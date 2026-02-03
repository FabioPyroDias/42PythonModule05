from typing import Any, List
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        if isinstance(data, (int)):
            return f"Processed 1 numeric value, sum={data}, avg={data}"
        return f"Processed {len(data)} numeric values, sum={sum(data)}, "
        f"avg={sum(data) / len(data):.1f}"

    def validate(self, data: Any) -> bool:
        if isinstance(data, List):
            for number in data:
                if not isinstance(number, (int)):
                    return False
            return True
        elif isinstance(data, int):
            return True
        else:
            return False

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class TextProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        char_index = 0
        found_word = False
        word_count = 0
        while char_index < len(data):
            if data[char_index] != ' ' and data[char_index] != '    ':
                found_word = True
            else:
                if found_word:
                    word_count += 1
                    found_word = False
            char_index += 1
        if found_word:
            word_count += 1
        return f"Processed text: {len(data)} characters, {word_count} words"

    def validate(self, data: Any) -> bool:
        if (isinstance(data, str) and "ERROR" not in data
                and "INFO" not in data):
            return True
        return False

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class LogProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        if data.find("ERROR"):
            return "[ALERT] ERROR level detected: Connection timeout"
        if data.find("INFO"):
            return "[INFO] INFO level detected: System ready"
        return "[INFO] Default status detected: All functions working"

    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        if data.find("ERROR") or data.find("INFO"):
            return True
        return False

    def format_output(self, result: str) -> str:
        return super().format_output(result)


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
    if processor_t.validate(data_tp):
        print("Validation: Text data verified")
        print(processor_t.format_output(processor_t.process(data_tp)))
    else:
        print("Validation: Invalid input")
    print()
    print("Initializing Log Processor...")
    processor_l = LogProcessor()
    data_lp = "ERROR: Connection timeou"
    print(f"Processing data: \"{data_lp}\"")
    if processor_l.validate(data_lp):
        print("Validation: Log entry verified")
        print(processor_l.format_output(processor_l.process(data_lp)))
    else:
        print("Validation: Invalid input")
    print()
    print("=== Polymorphic Processing Demo ===")
    print()
    print("Processing multiple data types through same interface...")
    processors = [
        processor_n,
        processor_t,
        processor_l
    ]
    data = [
        [1, 2, 3],
        "Hello World!",
        "INFO level detected: System ready"
    ]
    index = 0
    while index < len(processors):
        if processors[index].validate(data[index]):
            result = processors[index].process(data[index])
            print(f"Result {index + 1}: "
                  f"{processors[index].format_output(result)}")
        index += 1
    print()
    print("Foundation systems online. Nexus ready for advanced streams")
