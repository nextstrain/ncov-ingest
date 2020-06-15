#!/usr/bin/env python3
import fsspec
import pandas as pd

# Note: 'sequence' should NEVER appear in these lists!
METADATA_COLUMNS = [  # Ordering of columns in the existing metadata.tsv in the ncov repo
    'strain', 'virus', 'gisaid_epi_isl', 'genbank_accession', 'date', 'region',
    'country', 'division', 'location', 'region_exposure', 'country_exposure',
    'division_exposure', 'segment', 'length', 'host', 'age', 'sex', 'pangolin_lineage', 'GISAID_clade',
    'originating_lab', 'submitting_lab', 'authors', 'url', 'title', 'paper_url',
    'date_submitted'
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
            df[field] = pd.NA

    df.rename(column_mapper, axis='columns', inplace=True)

    # Normalize all string columns to Unicode Normalization Form C, for
    # consistent, predictable string comparisons.
    for column in df:
        if df[column].dtype == "string":
            df[column] = df[column].str.normalize("NFC").str.strip()

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
