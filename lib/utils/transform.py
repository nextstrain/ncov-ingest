#!/usr/bin/env python3
import regex
import fsspec
import pandas as pd
from datetime import datetime
from typing import List, Optional, Set, Union

# Note: 'sequence' should NEVER appear in these lists!
METADATA_COLUMNS = [  # Ordering of columns in the existing metadata.tsv in the ncov repo
    'strain', 'virus', 'gisaid_epi_isl', 'genbank_accession', 'date', 'region',
    'country', 'division', 'location', 'region_exposure', 'country_exposure',
    'division_exposure', 'segment', 'length', 'host', 'age', 'sex', 'pango_lineage', 'GISAID_clade',
    'originating_lab', 'submitting_lab', 'authors', 'url', 'title', 'paper_url',
    'date_submitted', 'sampling_strategy'
]


def standardize_dataframe(df: pd.DataFrame, column_mapper: dict) -> pd.DataFrame:
    """
    Standardize column names, column types and drop records where the
    sequence length is less than 15 kb then returns the modified DataFrame.
    """
    # Standardize to nullable dtypes
    df = df.convert_dtypes()

    # If an expected field is missing, fill it with NAs so the rest of the
    # script can assume it exists.
    for field in column_mapper:
        if field not in df:
            df[field] = pd.Series(dtype="string")

    df.rename(column_mapper, axis='columns', inplace=True)

    # Normalize all string columns to Unicode Normalization Form C, for
    # consistent, predictable string comparisons.
    for column in df:
        if df[column].dtype == "string":
            df[column] = df[column].str.normalize("NFC").str.strip()

    # Standardize date format to ISO 8601 date
    date_columns = {'date', 'date_submitted'}
    date_formats = {'%Y-%m-%d', '%Y-%m-%dT%H:%M:%SZ'}
    for column in date_columns:
        df[column] = df[column].apply(lambda x: format_date(x, date_formats))

    # Drop entries with length less than 15kb and reset index
    df = df \
        .drop(df[df["length"] < 15000].index) \
        .reset_index(drop=True)

    return df


def fill_default_geo_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill in default geographic metadata based on exisiting metadata if they
    have not been added by annotations.
    """
    # if division is blank, replace with country data, to avoid unexpected effects when subsampling by division
    # (where an empty division is counted as a 'division' group)
    df.loc[pd.isnull(df['division']), 'division'] = df['country']

    # Set `region_exposure` equal to `region` if it wasn't added by annotations
    if 'region_exposure' in df:
        df['region_exposure'].fillna(df['region'], inplace=True)
    else:
        df['region_exposure'] = df['region']

    # Set `country_exposure` equal to `country` if it wasn't added by annotations
    if 'country_exposure' in df:
        df['country_exposure'].fillna(df['country'], inplace=True)
    else:
        df['country_exposure'] = df['country']

    # Set `division_exposure` equal to `division` if it wasn't added by annotations
    if 'division_exposure' in df:
        df['division_exposure'].fillna(df['division'], inplace=True)
    else:
        df['division_exposure'] = df['division']

    return df


def write_fasta_file(sequence_data: pd.DataFrame, output_fasta: str):
    """ """
    with fsspec.open(str(output_fasta), 'wt') as fastafile:
        for index, row in sequence_data.iterrows():
            fastafile.write(f">{row['strain']}\n")
            fastafile.write(f"{row['sequence']}\n")


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
