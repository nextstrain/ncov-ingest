from typing import Container

from . import LINE_NUMBER_KEY
from ._base import Filter


class SequenceLengthFilter(Filter):
    def __init__(self, min_length: int):
        self.min_length = min_length

    def test_value(self, inp: dict) -> bool:
        return inp['length'] >= self.min_length


class LineNumberFilter(Filter):
    def __init__(self, line_numbers: Container[int]):
        self.line_numbers = line_numbers

    def test_value(self, inp: dict) -> bool:
        return inp[LINE_NUMBER_KEY] in self.line_numbers
