"""
Parse the GISAID JSON load into a metadata tsv and a FASTA file.
"""
import argparse
import json
import fsspec
import pandas as pd
from argparse import RawTextHelpFormatter
from pathlib import Path

# Note: 'sequence' should NEVER appear in this list!
METADATA_COLUMNS = [  # Ordering of columns in the existing metadata.tsv in the ncov repo
    'strain', 'virus', 'gisaid_epi_isl', 'genbank_accession', 'date', 'region',
    'country', 'division', 'location', 'segment', 'host', 'originating_lab',
    'submitting_lab', 'authors', 'url', 'title'
]

assert 'sequence' not in METADATA_COLUMNS, "Sequences should not appear in metadata!"

def preprocess(gisaid_data: pd.DataFrame) -> pd.DataFrame:
    """
    Renames columns and abbreviate strain name in a given *gisaid_data*
    DataFrame, returning the modified DataFrame.
    """
    mapper = {
        'covv_virus_name'   : 'strain',
        'covv_accession_id' : 'gisaid_epi_isl',
        'covv_subm_date'    : 'date',
        'covv_host'         : 'host',
        'covv_orig_lab'     : 'originating_lab',
        'covv_subm_lab'     : 'submitting_lab',
        'covv_authors'      : 'authors',
    }
    gisaid_data.rename(mapper, axis="columns", inplace=True)

    # Abbreviate strain names by removing the prefix. Strip spaces, too.
    gisaid_data['strain'] = gisaid_data['strain'] \
        .str.replace(r'^BetaCoV/', '', n=1, case=False) \
        .str.replace(r'\s', '')

    return gisaid_data.drop_duplicates(subset='strain')

def parse_geographic_columns(gisaid_data: pd.DataFrame) -> pd.DataFrame:
    """
    Expands the string found in the column named `covv_location` in the given
    *df*, creating four new columns. Returns the modified ``pd.DataFrame``.
    """
    geographic_data = gisaid_data['covv_location'].str.split('/', expand=True)

    gisaid_data['region']      = geographic_data[0]
    gisaid_data['country']     = geographic_data[1]
    gisaid_data['division']    = geographic_data[2]
    gisaid_data['location']    = geographic_data[3]

    return gisaid_data

def generate_hardcoded_metadata(hardcoded_metadata: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a ``pd.DataFrame`` with a column for strain ID plus additional
    columns containing harcoded metadata.
    """
    hardcoded_metadata = pd.DataFrame(gisaid_data['strain'])
    hardcoded_metadata['virus']             = 'ncov'
    hardcoded_metadata['genbank_accession'] = '?'
    hardcoded_metadata['url']               = 'https://www.gisaid.org'
    # TODO verify these are all actually true
    hardcoded_metadata['segment']           = 'genome'
    hardcoded_metadata['title']             = 'Newly discovered betacoronavirus, 2019-2020'

    return hardcoded_metadata

def write_fasta_file(sequence_data: pd.DataFrame):
    """ """
    sequence_data['sequence'] = sequence_data['sequence'].str.replace('\n', '')

    with fsspec.open(args.output_fasta, 'wt') as fastafile:
        for index, row in sequence_data.iterrows():
            fastafile.write(f">{row['strain']}\n")
            fastafile.write(f"{row['sequence']}\n\n")

def update_metadata(curated_gisaid_data: pd.DataFrame) -> pd.DataFrame:
    """ """
    # Add hardcoded metadata which is *almost* always true
    hardcoded_metadata = generate_hardcoded_metadata(curated_gisaid_data)
    curated_gisaid_data.update(hardcoded_metadata)
    curated_gisaid_data = curated_gisaid_data.merge(hardcoded_metadata)

    if args.metadata:
        # Use the curated metadata tsv to update any column values
        user_provided_metadata = pd.read_csv(args.metadata, header=None, sep='\t')
        for index, row in user_provided_metadata.iterrows():
            curated_gisaid_data.loc[curated_gisaid_data['strain'] == row[0], row[1]] = row[2]

    return curated_gisaid_data


if __name__ == '__main__':
    base = Path(__file__).resolve().parent.parent

    parser = argparse.ArgumentParser(
        description="Parse a GISAID JSON load into a metadata tsv and FASTA file.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("gisaid_data", help="Newline-delimited GISAID JSON data")
    parser.add_argument("--metadata",
        help="Optional manually curated metadata TSV.\n"
            "The TSV file should have no header and exactly three columns which contain:\n\t"
            "1. the strain ID\n\t"
            "2. the column name to replace from the generated `metadata.tsv` file\n\t"
            "3. the replacement data\n"
        "e.g.\n\t"
        "USA-MA1/2020    location    Boston\n\t"
        "USA/CA1/2020    genbank_accession   MN994467\n\t"
        "Wuhan-Hu-1/2019 collection_date 2019-12-26")
    parser.add_argument("--output-metadata",
        default=base / "data/metadata.tsv",
        help="Output location of generated metadata tsv. Defaults to `data/metadata.tsv`")
    parser.add_argument("--output-fasta",
        default=base / "data/sequences.fasta",
        help="Output location of generated FASTA file. Defaults to `data/sequences.fasta`")
    args = parser.parse_args()

    gisaid_data = pd.read_json(args.gisaid_data, lines=True)

    gisaid_data = preprocess(gisaid_data)

    write_fasta_file(gisaid_data)

    gisaid_data = parse_geographic_columns(gisaid_data)

    curated_gisaid_data = update_metadata(gisaid_data)

    # Reorder columns consistent with the existing metadata on GitHub
    curated_gisaid_data = curated_gisaid_data[METADATA_COLUMNS]
    curated_gisaid_data.to_csv(args.output_metadata, sep='\t', index=False)
