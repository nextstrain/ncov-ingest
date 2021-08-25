import csv
import re
import unicodedata
from collections import defaultdict
from typing import Any, Collection, List, MutableMapping, Sequence, Tuple , Dict , Union
import pandas as pd


from utils.transform import format_date, titlecase
from . import LINE_NUMBER_KEY
from ._base import Transformer



class UserProvidedGeoLocationSubstitutionRules:
    """ this class represents patterns of substitutions in the localisation data of entries """
    def __init__(self):
        self.entries: MutableMapping[str,MutableMapping[str, MutableMapping[str, MutableMapping[str, Tuple[str,str,str,str] ]]]] = defaultdict( lambda : defaultdict( lambda : defaultdict( dict ) ) )
        self.use_count: MutableMapping[Tuple[str,str,str,str], int] = dict()

    def readFromLine( self , line : str ) -> None:
        """ reads a substitution rule from a file line .
            This function primarily accepts a format : region/country/division/location<tab>region/country/division/location
            But it will also accept (an give a warning) if a line with all separators as tabs is given
        """

        if line.lstrip()[0] == '#':
            return # ignore comments

        row = line.strip('\n').split('\t')

        raw,annot = None,None

        if len(row) == 2:
            row[-1] = row[-1].partition('#')[0].rstrip()
            raw , annot = tuple( row[0].split('/') ) , tuple( row[1].split('/') )
        elif len(row) == 8:
            print("WARNING: found a rule line using the old all tabs separators format. This is accepted in that particular script but we do not guarantee it in any other.\nPlease update to region/country/division/location<tab>region/country/division/location.")
            row[-1] = row[-1].partition('#')[0].rstrip()
            raw , annot = tuple( row[:4] ) , tuple( row[4:8] )
        else:
            print("WARNING: couldn't decode rule line " + "\t".join(row))
            return
        #print('adding',raw,annot)
        self.add_user_rule( raw,annot )


    def add_user_rule(
            self,
            start: Tuple[str,str,str,str],
            arrival: Tuple[str,str,str,str],
    ) -> None:

        self.entries[ start[0] ][ start[1] ][ start[2] ][ start[3] ] = arrival

        self.use_count[start] = 0


    def findApplicableRule( self , start: Tuple[str,str,str,str] , current: List[str] = [None,None,None,None] , level : int = 0) -> Union[ Tuple[str,str,str,str] , None ]:
        """
        **recursive** up to 4 levels

        Takes:
            start: Tuple[str,str,str,str] : entry to find a rule for
            current: List[str,str,str,str] = [None,None,None,None] : current substituion pattern found, up until <level> index
            level : int = 0 : current index for which we try to find a rule

        Returns:
            Tuple[str,str,str,str] : completed substitution pattern
            or
            None : if no substittuion pattern was found
        """
        #print("findApplicableRule" , level , start , current)
        if level >= 4:
            return current

        ruleDic = self.entries
        for i in range(level):
            ruleDic = ruleDic[ current[i] ]

        if start[level] in ruleDic: # found a corresponding rule
            current[level] = start[level]
            rule = self.findApplicableRule(start, current , level+1)
            if not rule is None : # means a rule was found in all underlying levels
                return tuple(rule )

        if '*' in ruleDic: # if no corresponding rule was found, look up the general substitution rules
            current[level] = '*'
            rule = self.findApplicableRule(start, current , level+1)
            if not rule is None : # means a rule was found in all underlying levels
                return tuple(rule )

        #otherwise, no rule was found in the underlying levels
        return None





    def get_user_rules(self, start: Tuple[str,str,str,str] ) -> Tuple[str,str,str,str]:
        """
        NB: 1. will apply several rules if necessary (eg. transform A to B, then tranform B to C if rules A->B anf B->C exist)
            2. will apply general rules in the order regions, country, division, location.

            eg. if rules are :
                EU , * , * , * -> Europe , * , * , *
                Europe , France , Haut-De-France , * -> Europe , France , Haut de France , *

            then entry :
                EU , France , Haut-De-France , foo
            will first become
                Europe , France , Haut-De-France , foo
            and then
                Europe , France , Haut de France , foo


            Takes :
                - Tuple[str,str,str,str] : region, country, division, location tuple
            Returns :
                Tuple[str,str,str,str] -> tuple updated.
        """

        arrival = start
        rules_applied = 0
        continueApply = True
        while continueApply:
            continueApply = False

            rule = []
            rule = self.findApplicableRule( arrival , [None,None,None,None] )

            continueApply = not rule is None # we were able to form a full rule

            if continueApply:

                newArrival = self._replaceEntry( arrival , self.entries[ rule[0] ][rule[1]][rule[2]][rule[3]] )
                self.use_count[tuple( rule ) ] += 1
                rules_applied+=1

                #print("applied",rules_applied , ':', arrival , '->', rule , '->' , newArrival)
                if arrival == newArrival:
                    continueApply = False
                arrival = newArrival
            if rules_applied > 1000 :
                print("ERROR : more than 1000 geographic location rules applied on the same entry. There might be cyclicity in your rules")
                print("\tfaulty entry",start)
                exit(1)


        return arrival

    def _replaceEntry( self, start , arrival ):
        """ takes into account * character, which will not cause a change """
        new = list(start)
        for i in range(len(arrival)):
            if arrival[i] != '*':
                new[i] = arrival[i]
        return new

    def get_unused_annotations(self) -> Collection[str]:
        return [
            start
            for start, use_count in self.use_count.items()
            if use_count == 0
        ]


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

    def __init__(self , column_map = None) :
        self.column_map = column_map
        if self.column_map is None : # this default corresponds to column substituion for gisaid
            self.column_map = {
                'covv_virus_name': 'strain',
                'covv_accession_id': 'gisaid_epi_isl',
                'covv_collection_date': 'date',
                'covv_host': 'host',
                'covv_orig_lab': 'originating_lab',
                'covv_subm_lab': 'submitting_lab',
                'covv_authors': 'authors',
                'covv_patient_age': 'age',
                'covv_gender': 'sex',
                'covv_lineage': 'pango_lineage',
                'covv_clade': 'GISAID_clade',
                'covv_add_host_info': 'additional_host_info',
                'covv_add_location': 'additional_location_info',
                'covv_subm_date': 'date_submitted',
                'covv_location': 'location',
                'covv_sampling_strategy': 'sampling_strategy',
            }

    def transform_value(self, entry: dict) -> dict:
        for in_col, out_col in self.column_map.items():
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
        if entry['authors'] == "":
            entry['authors'] = '?'
        else:
            entry['authors'] = re.split(r'(?:\s*[,，;；]\s*|\s+(?:and|&)\s+)', entry['authors'])[0]

            if not entry['authors'].strip('. ').endswith(" et al"): # if it does not already finishes with " et al.", add it
                entry['authors'] += ' et al'

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
        epi_id = entry["gisaid_epi_isl"].upper()
        entry['virus'] = 'ncov'
        entry['genbank_accession'] = '?'
        if len(epi_id)>4: # gisaid epi ids don't have a fixed length, but check for at least length 4 to avoid a crash here
            entry['url'] = f'https://www.epicov.org/acknowledgement/{epi_id[-4:-2]}/{epi_id[-2:]}/{epi_id}.json'
        else:
            entry['url'] = 'https://gisaid.org'

        # TODO verify these are all actually true
        entry['segment'] = 'genome'
        entry['title'] = '?'
        entry['paper_url'] = '?'

        return entry


class MergeUserAnnotatedMetadata(Transformer):
    """Use the curated annotations tsv to update any column values."""
    def __init__(self, annotations: UserProvidedAnnotations , idKey : str = "gisaid_epi_isl"):
        self.annotations = annotations
        self.idKey = idKey

    def transform_value(self, entry: dict) -> dict:
        annotations = self.annotations.get_user_annotations( entry[ self.idKey ] )
        for key, value in annotations:
            if key in entry and entry[key] == value :
                print('REDUNDANT ANNOTATED METADATA :', entry[ self.idKey ] , key , value)

            entry[key] = value
        return entry

class ApplyUserGeoLocationSubstitutionRules(Transformer):
    """Use the curated subtitution rules tsv to update geographical column values."""
    def __init__(self, rules: UserProvidedGeoLocationSubstitutionRules):
        self.rules = rules

    def transform_value(self, entry: dict) -> dict:
        LOCATION_COLUMNS = ['region', 'country', 'division', 'location']
        newVal = self.rules.get_user_rules( tuple( [ entry[col] for col in LOCATION_COLUMNS ] ) )
        for i,key in enumerate(LOCATION_COLUMNS):
            entry[key] = newVal[i]
        return entry



class WriteCSV(Transformer):
    """writes the data to a CSV file."""
    def __init__(self, fileName: str ,
                 columns : List[str] ,
                 restval : str = '?' ,
                 extrasaction : str ='ignore' ,
                 delimiter : str = ',',
                 dict_writer_kwargs : Dict[str,str] = {} ):

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
        self.OUT.close()

    def transform_value(self, entry: dict) -> dict:
        self.writer.writerow(entry)
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

class StandardizeGenbankStrainNames(Transformer):
    """
    Attempt to standardize strain names by removing extra prefixes,
    stripping spaces, and correcting known common error patterns.

    If the strain name still does not have the expected format, default to the
    GenBank accession as the strain name.
    """
    def parse_strain_from_title(self,title: str) -> str:
        """
        Try to parse strain name from the given *title* using regex search.
        Returns an empty string if not match is found in the *title*.
        """
        strain_name_regex = r'[-\w]*/[-\w]*/[-\w]*\s'
        strain = re.search(strain_name_regex, title)
        return strain.group(0) if strain else ''

    def transform_value(self, entry: dict) -> dict:
        # Compile list of regex to be used for strain name standardization
        # Order is important here! Keep the known prefixes first!
        regex_replacement = [
            (r'(^SAR[S]{0,1}[-\s]{0,1}CoV[-]{0,1}2/|^2019[-\s]nCoV[-_\s/]|^BetaCoV/|^nCoV-|^hCoV-19/)',''),
            (r'(human/|homo sapien/|Homosapiens{0,1}/)',''),
            (r'^USA-', 'USA/'),
            (r'^USACT-', 'USA/CT-'),
            (r'^USAWA-', 'USA/WA-'),
            (r'^HKG.', 'HongKong/'),
        ]


        # Parse strain name from title to fill in strains that are empty strings
        entry['strain_from_title'] = self.parse_strain_from_title( entry['title'] )

        if entry['strain'] == '':
            entry['strain'] = entry['strain_from_title']


        # Standardize strain names using list of regex replacements
        for regex, replacement in regex_replacement:

            entry['strain'] = re.sub( regex, replacement, entry['strain'], flags=re.IGNORECASE)

        # Strip all spaces
        entry['strain'] = re.sub( r'\s', '' , entry['strain'] )

        # All strain names should have structure {}/{}/{year} or {}/{}/{}/{year}
        # with the exception of 'Wuhan-Hu-1/2019'
        # If strain name still doesn't match, default to the GenBank accession
        strain_name_regex = re.compile(  r'([\w]*/)?[\w]*/[-_\.\w]*/[\d]{4}' )
        if (( strain_name_regex.match( entry['strain'] ) is None ) and
            ( entry['strain'] != 'Wuhan-Hu-1/2019' )):
            entry['strain'] = entry['genbank_accession']

        return entry

class ParseGeographicColumnsGenbank(Transformer):
    """
    Expands string found in the column named `location` in the given
    *genbank_data* DataFrame, creating 3 new columns. Returns the modified
    DataFrame.

    Expected formats of the location string are:
        * "country"
        * "country: division"
        * "country: division, location"
    """
    def __init__(self, us_state_code_file_name ):
        # Create dict of US state codes and their full names
        self.us_states = pd.read_csv( us_state_code_file_name , header=None, sep='\t', comment="#")
        self.us_states = dict(zip(self.us_states[0], self.us_states[1]))


    def transform_value(self, entry : dict) -> dict :

        geographic_data = entry['location'].split(':')

        country = geographic_data[0].strip()
        division = ''
        location = ''

        if len(geographic_data) == 2 :
            division , j , location = geographic_data[1].partition(',')

        elif len(geographic_data) > 2:
            assert False, f"Found unknown format for geographic data: {value}"


        # Special parsing for US locations because the format varies
        if country == 'USA' and division:
            # Switch location & division if location is a US state
            if location and any(location.strip() in s for s in self.us_states.items()):
                state = location
                location = division
                division = state
            # Convert US state codes to full names
            if self.us_states.get(division.strip().upper()):
                division = self.us_states[division.strip().upper()]


        location = location.strip().lower().title() if location else ''
        division = division.strip().lower().title() if division else ''

        # fix German divisions
        for stripstr in ['Europe/', 'Germany/']:
            if division.startswith(stripstr):
                division = division[len(stripstr):]

        #print(entry , '->' , geographic_data , country, division, location)
        entry['country']     = country
        entry['division']    = division
        entry['location']    = location


        return entry

class AddHardcodedMetadataGenbank(Transformer):
    """
    Adds a key-value for strain ID plus additional key-values containing harcoded
    metadata.
    """
    def transform_value(self, entry: dict) -> dict:

        entry['virus']             = 'ncov'
        entry['gisaid_epi_isl']    = '?'
        entry['segment']           = 'genome'
        entry['age']               = '?'
        entry['sex']               = '?'
        entry['GISAID_clade']      = '?'
        entry['originating_lab']   = '?'
        entry['submitting_lab']    = '?'
        entry['paper_url']         = '?'
        entry['sampling_strategy']         = '?'

        entry['url'] = "https://www.ncbi.nlm.nih.gov/nuccore/" + entry['genbank_accession']
        return entry


class Tracker(Transformer):
    """
    here to print a number of entries when seen
    """
    def __init__(self , interestIds : set , interestField : str):
        self.interestIds = interestIds
        self.interestField = interestField

    def transform_value(self, entry : dict) -> dict :
        if entry[ self.interestField ] in self.interestIds:
            print(entry)
        return entry

class patchUKData(Transformer):
    """
    COG-UK explicitly suggests to use metadata provided om CLIMB to patch missing metadata.
    Below is a sample comment in the genbank record.
    ```
    COG_ACCESSION:COG-UK/BCNYJH/BIRM:20210316_1221_X5_FAP43200_37a8944f; COG_BASIC_QC COG_HIGH_QC:PASS;
    COG_NOTE:Sample metadata and QC flags may been updated since deposition in public databases.
    COG recommends users refer to data.covid19.climb.ac.uk for metadata and QC tables before
    conducting analysis.
    ```

    This transformer fetches the CLIMB data, matches records via sample accession, and fills in missing metadata
    """
    def __init__(self, sample_id_table, metadata_file):
        import uuid
        # load table with all sample IDs
        samples_ids = pd.read_csv(sample_id_table, sep='\t', index_col="central_sample_id")
        samples_ids = samples_ids.loc[~samples_ids.index.duplicated(keep='first')]

        # function to generated sample id from sequence name
        def get_sample_id(x):
            try:
                sample_id = x.split('/')[1]
            except:
                sample_id = f'problemsample_{str(uuid.uuid4())}'
            return sample_id

        # load metadata and produce IDs with no proper ID
        metadata = pd.read_csv(metadata_file, sep=',')[['sequence_name', 'country', 'adm1', 'is_pillar_2', 'sample_date','epi_week', 'lineage', 'lineages_version']]
        coguk_sample_ids = metadata.sequence_name.apply(get_sample_id)
        metadata.index=coguk_sample_ids
        metadata = metadata.loc[~metadata.index.duplicated(keep='first')]

        # merge tables and make a reduced table with unique sample accession (ena_sample.secondary_accession)
        merged_meta = pd.concat([samples_ids, metadata], axis='columns', copy=False).fillna('?')
        has_sample = merged_meta.loc[~merged_meta["ena_sample.secondary_accession"].isna()]
        has_sample.index = has_sample["ena_sample.secondary_accession"]

        self.metadata_lookup = {}
        geo_lookup = {'UK-ENG':('United Kingdom', 'England'),
                      'UK-SCT':('United Kingdom', 'Scotland'),
                      'UK-WLS':('United Kingdom', 'Wales'),
                      'UK-NIR':('United Kingdom', 'Northern Ireland')}
        for k,v in has_sample.iterrows():
            self.metadata_lookup[k]= {'strain':v['sequence_name'], 'date':v['sample_date'], 'pango_lineage':v['lineage'],
                                      'region':'Europe', 'country': 'United Kingdom', 'division':geo_lookup.get(v['adm1'],('?', '?'))[1],
                                      'gisaid_epi_isl':v['gisaid.accession']}

    def transform_value(self, entry: dict) -> dict:
        if entry["biosample_accession"] in self.metadata_lookup:
            entry.update(self.metadata_lookup[entry["biosample_accession"]])

        return entry


class ParseBiosample(Transformer):
    """
    Flattens the nested BioSample dictionary into a single level dictionary
    with standardized columns as keys.

    See all Biosample attributes at
    https://www.ncbi.nlm.nih.gov/biosample/docs/attributes/
    """
    def __init__(self, columns: List[str]):
        self.columns = columns

    # Multiple BioSample attribute fields can represent the same metadata.
    # We take the first value that matches the field regex from the most
    # standardized attribute field. The following metadata attribute fields
    # are listed in order with the most standardized first.
    #   -Jover, 2021-08-16
    STRAIN_ATTR = ['strain', 'isolate', 'sample_name', 'gisaid_virus_name', 'title']
    STRAIN_REGEX = r'([-\w\s]*/)?([-\w\s]*/)?[-\w\s]*/[-\w\s]*/[0-9]{4}$'

    ORIG_LAB_ATTR = ['collected_by', 'collecting institution', 'collecting institute']
    ORIG_LAB_REGEX = r'^(?!\s*$).+' # Matches any string that is not empty or just whitespace

    GISAID_EPI_ATTR = ['gisaid_accession', 'GISAID Accession ID', 'gisaid id', 'gisaid']
    GISAID_EPI_REGEX = r'EPI_ISL_[0-9]*'

    # Location metadata is handled differently since a subset of records split
    # the location metadata into multiple attributes
    #   -Jover, 2021-08-17
    LOCATION_ATTR = ['geo_loc_name', 'geographic location (region and locality)', 'region']

    # Potential BioSample values that represent null values
    NULL_VALUES = ['missing', 'nan', 'none', 'not applicable', 'not collected',
                   'not determined', 'not provided', 'restricted access', 'unknown']

    def parse_first_regex_match(self, regex: str, value: str) -> str:
        """
        Return the first regex match found in *value*.
        Returns an empty string if there is no match.
        """
        matches = re.search(regex, value)
        return matches.group(0) if matches else ''

    def parse_location(self, potential_values: Dict[str, str]) -> str:
        """
        Parse the location from the provided *potential_values*
        Returns empty string if no location data provided in *potential_values*
        """
        country = potential_values.get('geo_loc_name')
        division = potential_values.get('geographic location (region and locality)')\
                or potential_values.get('region')

        # A subset of records have full location in second field in the GISAID
        # format of region/country/division or region/country
        if country and division and country in division:
            division_parts = division.split('/')
            if len(division_parts) == 3:
                division = division_parts[-1]
            else:
                division = None

        if country and division:
            location = f'{country}: {division}'
        elif country:
            location = country
        else:
            location = ''

        return location

    def transform_value(self, entry: dict) -> dict:
        # Ensure all expected columns are included in the new entry
        new_entry = dict.fromkeys(self.columns, '')

        new_entry['biosample_accession'] = entry['accession']
        if entry.get('bioprojects'):
            # Arbitrarily chooses the first BioProject accession
            new_entry['bioproject_accession'] = entry['bioprojects'][0]['accession']

        # We can assume the owner/submitter of the BioSample record is the same as
        # the GenBank record because NCBI currently requires the same person/group
        # submit the linked records.
        # See: https://www.protocols.io/view/sars-cov-2-ncbi-consensus-submission-protocol-genb-bid7ka9n?step=2.5
        #   -Jover, 2021-08-16
        if (entry.get('owner') and
            entry['owner']['name'].lower() not in ParseBiosample.NULL_VALUES):
            new_entry['submitting_lab'] = entry['owner']['name']

        if entry.get('sampleIds'):
            for sample_id in entry['sampleIds']:
                if sample_id.get('db') == 'SRA':
                    new_entry['sra_accession'] = sample_id['value']
                else:
                    # If the sample ID is not from SRA, try to parse as strain name
                    new_entry['strain'] = self.parse_first_regex_match(ParseBiosample.STRAIN_REGEX, sample_id['value'])

        # Store the lowest index of the attribute field that we've processed
        # so that we use the value with the highest priority
        # Default with the length of the list of attribute fields
        strain_idx = len(ParseBiosample.STRAIN_ATTR)
        orig_lab_idx = len(ParseBiosample.ORIG_LAB_ATTR)
        gisaid_epi_idx = len(ParseBiosample.GISAID_EPI_ATTR)

        # Store all potential location values since they may need to be
        # combined to get the full location
        potential_locations = {}

        for attribute in entry['attributes']:
            key, value = attribute['name'], attribute['value']

            # Skip attributes that have null values
            if value.lower() in ParseBiosample.NULL_VALUES:
                continue

            # Seemingly standardized fields that do not have other field name variations
            #   -Jover, 2021-08-16
            if key == 'host_age':
                new_entry['age'] = value
            elif key == 'host_sex':
                new_entry['sex'] = value
            elif key == 'collection_date':
                new_entry['date'] = value

            # Store regex matched values for metadata if the field index is
            # lower than the field index of the stored value
            elif key in ParseBiosample.STRAIN_ATTR:
                field_idx = ParseBiosample.STRAIN_ATTR.index(key)
                value = self.parse_first_regex_match(ParseBiosample.STRAIN_REGEX, value)
                if field_idx < strain_idx and len(value) > 0:
                    strain_idx = field_idx
                    new_entry['strain'] = value

            elif key in ParseBiosample.ORIG_LAB_ATTR:
                field_idx = ParseBiosample.ORIG_LAB_ATTR.index(key)
                value = self.parse_first_regex_match(ParseBiosample.ORIG_LAB_REGEX, value)
                if field_idx < orig_lab_idx and len(value) > 0:
                    orig_lab_idx = field_idx
                    new_entry['originating_lab'] = value

            elif key in ParseBiosample.GISAID_EPI_ATTR:
                field_idx = ParseBiosample.GISAID_EPI_ATTR.index(key)
                value = self.parse_first_regex_match(ParseBiosample.GISAID_EPI_REGEX, value)
                if field_idx < gisaid_epi_idx:
                    gisaid_epi_idx = field_idx
                    new_entry['gisaid_epi_isl'] = value

            elif key in ParseBiosample.LOCATION_ATTR:
                potential_locations[key] = value

        new_entry['location'] = self.parse_location(potential_locations)

        return new_entry
