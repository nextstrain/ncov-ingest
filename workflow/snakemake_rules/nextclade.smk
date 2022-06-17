"""
This part of the workflow handles all rules related to NextClade.
Depends on the main Snakefile to define the variable `database`, which is NOT a wildcard.

Expects the following inputs:
    fasta = "data/{database}/sequences.fasta"
    existing_metadata = f"data/{database}/metadata_transformed.tsv"

Produces the following outputs:
    metadata = f"data/{database}/metadata.tsv"
    OPTIONAL OUTPUTS
    If there are new sequences not in the nextclade.tsv cache, the they will
    be run through NextClade to produce the following outputs:
        nextclade_info = f"data/{database}/nextclade.tsv"
        alignment = f"data/{database}/aligned.fasta"
"""

rule download_nextclade:
    params:
        dst_source = config["s3_dst"] + '/nextclade.tsv.gz',
        src_source = config["s3_src"] + '/nextclade.tsv.gz',
    output:
        nextclade = f"data/{database}/nextclade_old.tsv"
    shell:
        """
        ./bin/download-from-s3 {params.dst_source} {output.nextclade} ||  \
        ./bin/download-from-s3 {params.src_source} {output.nextclade}
        """

checkpoint get_sequences_without_nextclade_annotations:
    """Find sequences in FASTA which don't have clades assigned yet"""
    input:
        fasta = f"data/{database}/sequences.fasta",
        nextclade = f"data/{database}/nextclade_old.tsv",
    output:
        fasta = f"data/{database}/nextclade.sequences.fasta"
    shell:
        """
        ./bin/filter-fasta \
            --input_fasta={input.fasta} \
            --input_tsv={input.nextclade} \
            --output_fasta={output.fasta} \
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
        sequences = f"data/{database}/nextclade.sequences.fasta"
    params:
        nextclade_input_dir = temp(directory(f"data/{database}/nextclade_inputs")),
        nextclade_output_dir = temp(directory(f"data/{database}/nextclade")),
    threads: 64
    output:
        info = f"data/{database}/nextclade_new.tsv",
        alignment = temp(f"data/{database}/nextclade.aligned.upd.fasta"),
        insertions = temp(f"data/{database}/nextclade.insertions.csv")
    shell:
        """
        ./bin/run-nextclade \
            {input.sequences:q} \
            {output.info} \
            {params.nextclade_input_dir} \
            {params.nextclade_output_dir} \
            {output.alignment} \
            {output.insertions} \
            {GENES} \
            {threads}
        """

rule nextclade_info:
    message:
        """
        Generates nextclade info TSV for all sequences (new + old)
        """
    input:
        old_info = f"data/{database}/nextclade_old.tsv",
        new_info = f"data/{database}/nextclade_new.tsv"
    output:
        nextclade_info = f"data/{database}/nextclade.tsv"
    shell:
        """
        ./bin/join-rows \
            {input.old_info:q} \
            {input.new_info:q} \
            -o {output.nextclade_info:q}
        """

rule download_previous_alignment:
    ## NOTE two potential bugs with this implementation:
    ## (1) race condition. This file may be updated on the remote after download_nextclade has run but before this rule
    ## (2) we may get `download_nextclade` and `download_previous_alignment` from different s3 buckets
    params:
        dst_source = config["s3_dst"] + '/aligned.fasta.xz',
        src_source = config["s3_src"] + '/aligned.fasta.xz',
    output:
        alignment = temp(f"data/{database}/nextclade.aligned.old.fasta")
    shell:
        """
        ./bin/download-from-s3 {params.dst_source} {output.alignment} ||  \
        ./bin/download-from-s3 {params.src_source} {output.alignment}
        """

rule combine_alignments:
    message:
        """
        Generating full alignment by combining newly aligned sequences with previous (cached) alignment
        """
    input:
        old_alignment = f"data/{database}/nextclade.aligned.old.fasta",
        new_alignment = f"data/{database}/nextclade.aligned.upd.fasta"
    output:
        alignment = f"data/{database}/aligned.fasta"
    shell:
        """
        cat {input.old_alignment} {input.new_alignment} > {output.alignment}
        """

def _get_nextclade_info(wildcards):
    ## the nextclade metadata should represent the entire dataset. If there are new sequences
    ## this has to be generated; if not then we can use the previous (cached) file.
    nextclade_sequences_path = checkpoints.get_sequences_without_nextclade_annotations.get().output.fasta
    if os.path.getsize(nextclade_sequences_path) > 0:
        return f"data/{database}/nextclade.tsv"
    return f"data/{database}/nextclade_old.tsv"

rule generate_metadata:
    input:
        existing_metadata = f"data/{database}/metadata_transformed.tsv",
        new_metadata = _get_nextclade_info
    output:
        metadata = f"data/{database}/metadata.tsv"
    # note: the shell scripts which predated this snakemake workflow
    # overwrote the existing_metadata here
    shell:
        """
        ./bin/join-metadata-and-clades \
            {input.existing_metadata} \
            {input.new_metadata} \
            -o {output.metadata}
        """