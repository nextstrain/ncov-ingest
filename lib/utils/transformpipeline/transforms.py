import re
import unicodedata
from collections import defaultdict
from typing import Any, Collection, List, MutableMapping, Sequence, Tuple

from utils.transform import format_date, titlecase
from . import LINE_NUMBER_KEY
from ._base import Transformer


class UserProvidedAnnotations:
    def __init__(self):
        self.entries: MutableMapping[str, List[Tuple[str, Any]]] = defaultdict(list)
        self.use_count: MutableMapping[str, int] = dict()

    def add_user_annotation(
            self,
            gisaid_epi_isl: str,
            key: str,
            value: Any,
    ) -> None:
        self.entries[gisaid_epi_isl].append((key, value))
        self.use_count[gisaid_epi_isl] = 0

    def get_user_annotations(self, gisaid_epi_isl: str) -> Sequence[Tuple[str, Any]]:
        annotations = self.entries.get(gisaid_epi_isl, None)
        if annotations is not None:
            self.use_count[gisaid_epi_isl] += 1
            return annotations
        else:
            return []

    def get_unused_annotations(self) -> Collection[str]:
        return [
            gisaid_epi_isl
            for gisaid_epi_isl, use_count in self.use_count.items()
            if use_count == 0
        ]


class RenameAndAddColumns(Transformer):
    """This transformer applies the column renames as dictated by COLUMN_MAP."""

    COLUMN_MAP = {
        'covv_virus_name': 'strain',
        'covv_accession_id': 'gisaid_epi_isl',
        'covv_collection_date': 'date',
        'covv_host': 'host',
        'covv_orig_lab': 'originating_lab',
        'covv_subm_lab': 'submitting_lab',
        'covv_authors': 'authors',
        'covv_patient_age': 'age',
        'covv_gender': 'sex',
        'covv_lineage': 'pangolin_lineage',
        'covv_clade': 'GISAID_clade',
        'covv_add_host_info': 'additional_host_info',
        'covv_add_location': 'additional_location_info',
        'covv_subm_date': 'date_submitted',
        'covv_location': 'location',
    }

    def transform_value(self, entry: dict) -> dict:
        for in_col, out_col in RenameAndAddColumns.COLUMN_MAP.items():
            if in_col not in entry:
                entry[out_col] = ""
            else:
                entry[out_col] = entry.pop(in_col)
        return entry


class StandardizeData(Transformer):
    """This transformer standardizes the data format:

    1. Removes newlines from the sequence and measures its length.
    2. Strip whitespace and convert to Unicode Normalization Form C for all strings.
    3. Standardize date formats.
    4. Abbreviate and remove whitespace from strain names3
    5. Add a line number.
    """

    def __init__(self):
        self.line_count = 1

    def transform_value(self, entry: dict) -> dict:
        entry['sequence'] = entry['sequence'].replace('\n', '')
        entry['length'] = len(entry['sequence'])

        # Normalize all string data to Unicode Normalization Form C, for
        # consistent, predictable string comparisons.
        str_kvs = {
            key: unicodedata.normalize('NFC', value).strip()
            for key, value in entry.items()
            if isinstance(value, str)
        }
        entry.update(str_kvs)

        # Standardize date format to ISO 8601 date
        date_columns = {'date', 'date_submitted'}
        date_formats = {'%Y-%m-%d', '%Y-%m-%dT%H:%M:%SZ'}
        for column in date_columns:
            entry[column] = format_date(entry[column], date_formats)

        # Abbreviate strain names by removing the prefix. Strip spaces, too.
        entry['strain'] = re.sub(
            r'(^[hn]CoV-19/)|\s+', '', entry['strain'], flags=re.IGNORECASE)

        entry[LINE_NUMBER_KEY] = self.line_count
        self.line_count += 1

        return entry


class DropSequenceData(Transformer):
    """This transformer drops the sequence data.  This is necessary to read the entire
    stream into memory to sort and deduplicate."""
    def transform_value(self, entry: dict) -> dict:
        entry.pop('sequence')
        return entry


class ExpandLocation(Transformer):
    """
    Expands the string found under the key `location`, creating four new values.
    """
    # Manually curate set of tokens that should not be cast to title case
    ARTICLES = {
        'and', 'de', 'del', 'des', 'di', 'do', 'en', 'l', 'la', 'las', 'le', 'los',
        'nad', 'of', 'op', 'sur', 'the', 'y'
    }
    ABBREV = {'USA', 'DC'}
    LOCATION_COLUMNS = ['region', 'country', 'division', 'location']

    def transform_value(self, entry: dict) -> dict:
        geographic_data = entry['location'].split(
            '/',
            maxsplit=len(ExpandLocation.LOCATION_COLUMNS))
        geographic_data += [""] * (
                len(ExpandLocation.LOCATION_COLUMNS) - len(geographic_data))

        for index, column in enumerate(ExpandLocation.LOCATION_COLUMNS):
            entry[column] = titlecase(
                geographic_data[index]
                .replace('_', ' ')
                .strip(),
                ExpandLocation.ARTICLES,
                ExpandLocation.ABBREV,
            )

        return entry


class FixLabs(Transformer):
    """
    Clean up and fix common spelling mistakes for labs.
    """

    def transform_value(self, entry: dict) -> dict:
        for lab_key in ('originating_lab', 'submitting_lab'):
            entry[lab_key] = FixLabs._cleanup_value(entry[lab_key])
        return entry

    @staticmethod
    def _cleanup_value(val: str) -> str:
        return (
            re.sub(r'\s+', ' ', val)
                .replace("Contorl", "Control")
                .replace("Dieases", "Disease")
        )


class AbbreviateAuthors(Transformer):
    """
    Abbreviates the column named `authors` to be "<first author> et al" rather
    than a full list.

    This is a "best effort" approach and still mangles a bunch of things.
    Without structured author list data, improvements to the automatic parsing
    meet diminishing returns quickly.  Further improvements should be
    considered using manual annotations of an author map (separate from our
    existing corrections/annotations).
    """
    def transform_value(self, entry: dict) -> dict:
        # Strip and normalize whitespace
        entry['authors'] = re.sub(r'\s+', ' ', entry['authors'])
        entry['authors'] = re.split(r'(?:\s*[,，;；]\s*|\s+(?:and|&)\s+)', entry['authors'])[0] + " et al"
        return entry


class ParsePatientAge(Transformer):
    """
    Parse patient age.
    """
    def transform_value(self, entry: dict) -> dict:
        # Convert "60s" or "50's" to "?"
        entry['age'] = re.sub(r'^\d+\'?[A-Za-z]', "?", entry['age'])
        # Convert to just digit
        entry['age'] = re.sub(r'^(\d+) years$', r'\1', entry['age'])
        # Convert months to years
        match = re.match(r'^(\d+) months', entry['age'])
        if match:
            entry['age'] = str(int(match.group(1)) / 12.0)
        # Cleanup unknowns
        entry['age'] = re.sub(r'^0$', '?', entry['age'])
        # Convert numeric values to int and convert non-numeric values to "?"
        try:
            entry['age'] = int(float(entry['age']))
        except ValueError:
            entry['age'] = "?"
        return entry


class ParseSex(Transformer):
    """
    Parse patient sex.
    """
    def transform_value(self, entry: dict) -> dict:
        # Casing, abbreviations, and spelling
        entry['sex'] = re.sub(r"^(male|M)$", "Male", entry['sex'])
        entry['sex'] = re.sub(r"^(female|F|Femal)$", "Female", entry['sex'])
        # Cleanup unknowns
        entry['sex'] = re.sub(r"^(unknown|N/A|NA|not applicable)$", "?", entry['sex'])
        return entry


class AddHardcodedMetadata(Transformer):
    """
    Adds a key-value for strain ID plus additional key-values containing harcoded
    metadata.
    """
    def transform_value(self, entry: dict) -> dict:
        entry['virus'] = 'ncov'
        entry['genbank_accession'] = '?'
        entry['url'] = 'https://www.gisaid.org'
        # TODO verify these are all actually true
        entry['segment'] = 'genome'
        entry['title'] = '?'
        entry['paper_url'] = '?'

        return entry


class MergeUserAnnotatedMetadata(Transformer):
    """Use the curated annotations tsv to update any column values."""
    def __init__(self, annotations: UserProvidedAnnotations):
        self.annotations = annotations

    def transform_value(self, entry: dict) -> dict:
        annotations = self.annotations.get_user_annotations(entry['gisaid_epi_isl'])
        for key, value in annotations:
            entry[key] = value
        return entry


class FillDefaultLocationData(Transformer):
    """
    Fill in default geographic metadata based on existing metadata if they have not been
    added by annotations.
    """
    def transform_value(self, entry: dict) -> dict:
        # if division is blank, replace with country data, to avoid unexpected effects
        # when subsampling by division (where an empty division is counted as a
        # 'division' group)
        if len(entry['division']) == 0:
            entry['division'] = entry['country']

        # Set `region_exposure` equal to `region` if it wasn't added by annotations
        entry.setdefault('region_exposure', entry['region'])

        # Set `country_exposure` equal to `country` if it wasn't added by annotations
        entry.setdefault('country_exposure', entry['country'])

        # Set `division_exposure` equal to `division` if it wasn't added by annotations
        entry.setdefault('division_exposure', entry['division'])

        return entry
