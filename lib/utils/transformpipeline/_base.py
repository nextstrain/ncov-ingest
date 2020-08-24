"""
The classes in this module allow modular definitions of transformations and filters when
processing a stream of data.  Most of the complexity is to allow for the simple
representation of such streams.  An example might be:

RenameColumns() | CalculateLength() | FilterBasedOnLength()

A more vanilla implementation might be simpler, but is less intuitive to use:

FilterBasedOnLength().process(CalculateLength().process(RenameColumns().process()))
"""


from abc import abstractmethod
from typing import cast, Iterable, Iterator


class PipelineException(Exception):
    pass


class DataSourceIterator(Iterator[dict]):
    """A data source iterator represents the read marker for a stream
    (i.e., an Iterable) of dicts."""
    @abstractmethod
    def raise_exception(self, exc: Exception) -> bool:
        """Returns true if an exception should be raised."""
        pass


class DataSource(Iterable[dict]):
    """A data source represents a stream (i.e., an Iterable) of dicts."""
    def __or__(self, other):
        return ChainedPipelineComponent(self, other)

    @abstractmethod
    def __iter__(self) -> DataSourceIterator:
        pass


class PipelineComponent:
    """A pipeline component transforms an input stream of dicts to an output stream of
    dicts.   Each pipeline component represents a step in the processing of gisaid data.
    """
    @abstractmethod
    def process(self, iterator: Iterator[dict]) -> dict:
        pass


class ChainedPipelineComponentIterator(DataSourceIterator):
    def __init__(
            self,
            data_source: DataSource,
            pipe_component: PipelineComponent,
    ):
        self.data_source_iterator = cast(DataSourceIterator, iter(data_source))
        self.pipe_component = pipe_component

    def __next__(self) -> dict:
        try:
            return self.pipe_component.process(self.data_source_iterator)
        except StopIteration:
            raise
        except PipelineException:
            raise
        except Exception as ex:
            if self.data_source_iterator.raise_exception(ex):
                raise

    def raise_exception(self, exc: Exception) -> bool:
        return self.data_source_iterator.raise_exception(exc)


class ChainedPipelineComponent(DataSource):
    """A chained pipeline component represents a data source and a pipeline component.
    This itself is a data source that can be fed into the next pipeline component.
    """
    def __init__(
            self,
            data_source: DataSource,
            pipe_component: PipelineComponent,
    ):
        self.data_source = data_source
        self.pipe_component = pipe_component

    def __iter__(self) -> DataSourceIterator:
        return ChainedPipelineComponentIterator(self.data_source, self.pipe_component)


class Transformer(PipelineComponent):
    """A transformer is a pipeline component that transforms each value in the input
    stream.  Implementations should implement `transform_value`, which takes a single
    value in the stream an outputs the value."""
    def process(self, iterator: Iterator[dict]) -> dict:
        return self.transform_value(next(iterator))

    @abstractmethod
    def transform_value(self, entry: dict) -> dict:
        pass


class Filter(PipelineComponent):
    """A filter is a pipeline component that tests whether each value in the input
    stream should be in the output stream.  Implementations should implement
    `test_value`, which should return True if the value should be in the output stream.
    """
    def process(self, iterator: Iterator[dict]) -> dict:
        while True:
            entry = next(iterator)
            if self.test_value(entry):
                return entry

    @abstractmethod
    def test_value(self, entry: dict) -> bool:
        pass
