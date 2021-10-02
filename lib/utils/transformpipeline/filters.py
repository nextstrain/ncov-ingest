from typing import Container , List, Dict
import csv

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


class GenbankProblematicFilter(Filter):
    """
    Find records that are missing geographic regions or country to exclude them
    from the final output and print them out separately for manual curation.
    """
    def __init__(self, fileName: str ,
                 columns : List[str] ,
                 restval : str = '?' ,
                 extrasaction : str ='ignore' ,
                 delimiter : str = ',',
                 dict_writer_kwargs : Dict[str,str] = {} ):

        self.printProblem = fileName !=''
        if self.printProblem:
            self.OUT = open( fileName , 'wt')

            self.writer = csv.DictWriter(
                self.OUT,
                columns,
                restval=restval,
                extrasaction=extrasaction,
                delimiter=delimiter,
                **dict_writer_kwargs
            )
            self.writer.writeheader()

    def __del__(self):
        if self.printProblem:
            self.OUT.close()


    def test_value(self, inp: dict) -> bool:

        OK = True

        if inp['region'] == '':
            OK = False
        elif inp['country'] == '':
            OK = False

        if not OK and self.printProblem :
            self.writer.writerow(inp)

        return OK


