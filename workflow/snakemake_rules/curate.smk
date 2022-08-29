"""
This part of the workflow handles the data transformation and curation.

Expects different inputs for GISAID vs GenBank:
    GISAID:
        ndjson = "data/gisaid.ndjson"
    GenBank:
        ndjson = "data/genbank.ndjson"
        biosample = "data/biosample.ndjson"

Produces different output files for GISAID vs GenBank:
    GISAID:
        fasta = "data/gisaid/sequences.fasta"
        metadata = "data/gisaid/metadata_transformed.tsv"
        flagged_annotations = temp("data/gisaid/flagged-annotations")
        duplicate_biosample = "data/gisaid/duplicate_biosample.txt"
        flagged_metadata = "data/gisaid/flagged_metadata.txt"
    GenBank:
        fasta = "data/genbank/sequences.fasta"
        metadata = "data/genbank/metadata_transformed.tsv"
        flagged_annotations = temp("data/genbank/flagged-annotations")
        duplicate_biosample = "data/genbank/duplicate_biosample.txt"
"""

rule transform_biosample:
    input:
        biosample = "data/biosample.ndjson"
    output:
        biosample = "data/genbank/biosample.tsv"
    shell:
        """
        ./bin/transform-biosample {input.biosample} \
            --output {output.biosample}
        """

rule transform_genbank_data:
    input:
        biosample = "data/genbank/biosample.tsv",
        ndjson = "data/genbank.ndjson"
    output:
        fasta = "data/genbank/sequences.fasta",
        metadata = "data/genbank/metadata_transformed.tsv",
        flagged_annotations = temp("data/genbank/flagged-annotations"),
        duplicate_biosample = "data/genbank/duplicate_biosample.txt"
    shell:
        """
        ./bin/transform-genbank {input.ndjson} \
            --biosample {input.biosample} \
            --duplicate-biosample {output.duplicate_biosample} \
            --output-metadata {output.metadata} \
            --output-fasta {output.fasta} > {output.flagged_annotations}
        """

rule transform_gisaid_data:
    input:
        ndjson = "data/gisaid.ndjson"
    output:
        fasta = "data/gisaid/sequences.fasta",
        metadata = "data/gisaid/metadata_transformed.tsv",
        flagged_annotations = temp("data/gisaid/flagged-annotations"),
        additional_info = "data/gisaid/additional_info.tsv"
    shell:
        """
        ./bin/transform-gisaid {input.ndjson} \
            --output-metadata {output.metadata} \
            --output-fasta {output.fasta}  \
            --output-additional-info {output.additional_info} \
            --output-unix-newline > {output.flagged_annotations};
        """

rule flag_metadata:
    ### only applicable for GISAID
    input:
        metadata = "data/gisaid/metadata.tsv"
    output:
        metadata = "data/gisaid/flagged_metadata.txt"
    resources:
        # Memory use scales primarily with the size of the metadata file.
        mem_mb=20000
    shell:
        """
        ./bin/flag-metadata {input.metadata} > {output.metadata}
        """
