#!/usr/bin/env python3
import regex
import pandas as pd
from datetime import datetime
from typing import Optional, Set, Union

# Note: 'sequence' should NEVER appear in these lists!
METADATA_COLUMNS = [  # Ordering of columns in the existing metadata.tsv in the ncov repo
    'strain', 'virus', 'gisaid_epi_isl', 'genbank_accession', 'genbank_accession_rev', 'sra_accession', 'date', 'region',
    'country', 'division', 'location', 'region_exposure', 'country_exposure',
    'division_exposure', 'segment', 'length', 'host', 'age', 'sex', 'pango_lineage', 'GISAID_clade',
    'originating_lab', 'submitting_lab', 'authors', 'url', 'title', 'paper_url',
    'date_submitted', 'date_updated','sampling_strategy'
]


def titlecase(text: Union[str, pd._libs.missing.NAType],
    articles: Set[str] = {}, abbrev: Set[str] = {}) -> Optional[str]:
    """
    Returns a title cased location name from the given location name
    *tokens*. Ensures that no tokens contained in the *whitelist_tokens* are
    converted to title case.

    >>> articles = {'a', 'and', 'of', 'the', 'le'}
    >>> abbrev = {'USA', 'DC'}

    >>> titlecase("the night OF THE LIVING DEAD", articles)
    'The Night of the Living Dead'

    >>> titlecase("BRAINE-LE-COMTE, FRANCE", articles)
    'Braine-le-Comte, France'

    >>> titlecase("auvergne-RHÔNE-alpes", articles)
    'Auvergne-Rhône-Alpes'

    >>> titlecase("washington DC, usa", articles, abbrev)
    'Washington DC, USA'
    """
    if not isinstance(text, str):
        return

    words = enumerate(regex.split(r'\b', text, flags=regex.V1))

    def changecase(index, word):
        casefold = word.casefold()
        upper = word.upper()

        if upper in abbrev:
            return upper
        elif casefold in articles and index != 1:
            return word.lower()
        else:
            return word.title()

    return ''.join(changecase(i, w) for i, w in words)


def format_date(date_string: str, expected_formats: set) -> str:
    """
    Format *date_string* to ISO 8601 date (YYYY-MM-DD).
    If *date_string* does not match *expected_formats*, return *date_string*.

    >>> expected_formats = {'%Y-%m-%d', '%Y-%m-%dT%H:%M:%SZ'}

    >>> format_date("2020", expected_formats)
    '2020'

    >>> format_date("2020-01", expected_formats)
    '2020-01'

    >>> format_date("2020-1-15", expected_formats)
    '2020-01-15'

    >>> format_date("2020-1-1", expected_formats)
    '2020-01-01'

    >>> format_date("2020-01-15", expected_formats)
    '2020-01-15'

    >>> format_date("2020-01-15T00:00:00Z", expected_formats)
    '2020-01-15'
    """
    for date_format in expected_formats:
        try:
            return datetime.strptime(date_string, date_format).strftime('%Y-%m-%d')
        except ValueError:
            continue

    return date_string
