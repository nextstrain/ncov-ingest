"""
This part of the workflow handles all rules related to NextClade.
Depends on the main Snakefile to define the variable `database`, which is NOT a wildcard.

Expects the following inputs:
    fasta = "data/{database}/sequences.fasta"
    existing_metadata = f"data/{database}/metadata_transformed.tsv"

    OPTIONAL INPUTS
    If not downloading NextClade cache files from AWS S3 (not providing `s3_dst` and `s3_src` in config),
    then empty cache file will be generated. Users can optionally include local
    cache files to satisfy the Snakemake input requirements:
        old_info = f"data/{database}/nextclade_old.tsv"
        old_alignment = f"data/{database}/nextclade.aligned.old.fasta"

Produces the following outputs:
    metadata = f"data/{database}/metadata.tsv"
    OPTIONAL OUTPUTS
    If there are new sequences not in the nextclade.tsv cache, the they will
    be run through NextClade to produce the following outputs:
        nextclade_info = f"data/{database}/nextclade.tsv"
        alignment = f"data/{database}/aligned.fasta"
"""


rule create_empty_nextclade_info:
    message:
        """Creating empty NextClade info cache file"""
    output:
        touch(f"data/{database}/nextclade_old.tsv"),


rule create_empty_nextclade_aligned:
    message:
        """Creating empty NextClade aligned cache file"""
    output:
        touch(f"data/{database}/nextclade.aligned.old.fasta"),


# Only include rules to fetch from S3 if S3 config params are provided
if config.get("s3_dst") and config.get("s3_src"):

    # Set ruleorder since these rules have the same output
    # Allows us to only download the NextClade cache from S3 only if the
    # S3 parameters are provided in the config.
    ruleorder: download_nextclade > create_empty_nextclade_info
    ruleorder: download_previous_alignment > create_empty_nextclade_aligned

    rule download_nextclade:
        params:
            dst_source=config["s3_dst"] + "/nextclade.tsv.xz",
            src_source=config["s3_src"] + "/nextclade.tsv.xz",
        output:
            nextclade=f"data/{database}/nextclade_old.tsv",
        shell:
            """
            ./bin/download-from-s3 {params.dst_source} {output.nextclade} ||  \
            ./bin/download-from-s3 {params.src_source} {output.nextclade}
            """

    rule download_previous_alignment:
        params:
            dst_source=config["s3_dst"] + "/aligned.fasta.xz",
            src_source=config["s3_src"] + "/aligned.fasta.xz",
        output:
            alignment=temp(f"data/{database}/nextclade.aligned.old.fasta"),
        shell:
            """
            ./bin/download-from-s3 {params.dst_source} {output.alignment} ||  \
            ./bin/download-from-s3 {params.src_source} {output.alignment}
            """


"""
NOTE two potential bugs with this implementation:
(1) race condition. This file may be updated on the remote after download_nextclade has run but before this rule
(2) we may get `download_nextclade` and `download_previous_alignment` from different s3 buckets
"""


rule get_sequences_without_nextclade_annotations:
    """Find sequences in FASTA which don't have clades assigned yet"""
    input:
        fasta=f"data/{database}/sequences.fasta",
        nextclade=f"data/{database}/nextclade_old.tsv",
    output:
        fasta=f"data/{database}/nextclade.sequences.fasta",
    shell:
        """
        if [[ -s {input.nextclade} ]]; then
            ./bin/filter-fasta \
                --input_fasta={input.fasta} \
                --input_tsv={input.nextclade} \
                --output_fasta={output.fasta}
        else
            cp {input.fasta} {output.fasta}
        fi
        """


rule print_number_of_sequences_without_nextclade_annotations:
    """Print number of sequences in FASTA which don't have clades assigned yet"""
    input:
        fasta=f"data/{database}/nextclade.sequences.fasta",
    output:
        touch(f"data/{database}/nextclade.sequences.fasta.count"),
    shell:
        """
        echo "[ INFO] Number of sequences to run Nextclade on: $(grep -c '^>' {input.fasta})"
        """


GENES = "E,M,N,ORF1a,ORF1b,ORF3a,ORF6,ORF7a,ORF7b,ORF8,ORF9b,S"


GENES_SPACE_DELIMITED = GENES.replace(",", " ")


rule run_nextclade:
    message:
        """
        Runs nextclade on sequences which were not in the previously cached nextclade run.
        This alignes sequences, assigns clades and calculates some of the other useful
        metrics which will ultimately end up in metadata.tsv.
        """
    input:
        sequences=f"data/{database}/nextclade.sequences.fasta",
    params:
        nextclade_input_dir=temp(directory(f"data/{database}/nextclade_inputs")),
        nextclade_output_dir=temp(directory(f"data/{database}/nextclade")),
    output:
        info=f"data/{database}/nextclade_new.tsv",
        alignment=temp(f"data/{database}/nextclade.aligned.upd.fasta"),
        insertions=temp(f"data/{database}/nextclade.insertions.csv"),
    shell:
        """
        ./bin/run-nextclade \
            {input.sequences:q} \
            {output.info} \
            {params.nextclade_input_dir} \
            {params.nextclade_output_dir} \
            {output.alignment} \
            {output.insertions} \
            {GENES}
        """


rule nextclade_info:
    message:
        """
        Generates nextclade info TSV for all sequences (new + old)
        """
    input:
        old_info=f"data/{database}/nextclade_old.tsv",
        new_info=f"data/{database}/nextclade_new.tsv",
    output:
        nextclade_info=f"data/{database}/nextclade.tsv",
    shell:
        """
        if [[ -s {input.old_info} ]]; then
            ./bin/join-rows \
                {input.old_info:q} \
                {input.new_info:q} \
                -o {output.nextclade_info:q}
        else
            mv {input.new_info} {output.nextclade_info}
        fi
        """


rule combine_alignments:
    message:
        """
        Generating full alignment by combining newly aligned sequences with previous (cached) alignment
        """
    input:
        old_alignment=f"data/{database}/nextclade.aligned.old.fasta",
        new_alignment=f"data/{database}/nextclade.aligned.upd.fasta",
    output:
        alignment=f"data/{database}/aligned.fasta",
    shell:
        """
        if [[ -s {input.old_alignment} ]]; then
            mv {input.old_alignment} {output.alignment}
            cat {input.new_alignment} >> {output.alignment}
        else
            mv {input.new_alignment} {output.alignment}
        fi
        """


rule generate_metadata:
    input:
        nextclade_tsv=f"data/{database}/nextclade.tsv",
        aligned_fasta=f"data/{database}/aligned.fasta",
        existing_metadata=f"data/{database}/metadata_transformed.tsv",
        trigger_count=f"data/{database}/nextclade.sequences.fasta.count",
    output:
        metadata=f"data/{database}/metadata.tsv",
    # note: the shell scripts which predated this snakemake workflow
    # overwrote the existing_metadata here
    shell:
        """
        ./bin/join-metadata-and-clades \
            {input.existing_metadata} \
            {input.nextclade_tsv} \
            -o {output.metadata}
        """
