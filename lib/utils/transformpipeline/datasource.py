import json
from typing import Iterable

from ._base import DataSource, DataSourceIterator, PipelineException


class LineToJsonIterator(DataSourceIterator):
    def __init__(self, lines: Iterable[str]):
        self.lines_iter = iter(lines)
        self.last_line = None
        self.lines_exceptions_raised = set()

    def __next__(self) -> dict:
        self.last_line = next(self.lines_iter)
        return json.loads(self.last_line)

    def raise_exception(self, exc: Exception) -> bool:
        raise PipelineException(f"Error parsing line:\n{self.last_line}")


class LineToJsonDataSource(DataSource):
    """This data source takes an iterable of json lines (i.e., ndjson) and produces
    an iterator of parsed objects."""
    def __init__(self, lines: Iterable[str]):
        self.lines = lines

    def __iter__(self) -> DataSourceIterator:
        return LineToJsonIterator(self.lines)
