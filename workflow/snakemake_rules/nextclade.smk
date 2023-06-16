"""
This part of the workflow handles all rules related to NextClade.
Depends on the main Snakefile to define the variable `database`, which is NOT a wildcard.

We run Nextclade twice, once on the normal sars-cov-2 dataset and once on the 21L sars-cov-2-21L dataset.
To keep the Snakefile dry, we use a wildcard `{reference}` that is either empty or `_21L`.
Since alignments are identical, we don't merge and upload the 21L alignments to S3.
21L outputs are used for `immune_escape` and `ace2_binding` columns.

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
from shlex import quote as shellquote


wildcard_constraints:
    reference="|_21L",
    seqtype="[^.]+",


rule create_empty_nextclade_info:
    message:
        """Creating empty NextClade info cache file"""
    output:
        touch(f"data/{database}/nextclade{{reference}}_old.tsv"),


rule create_empty_nextclade_aligned:
    message:
        """Creating empty NextClade aligned cache file"""
    output:
        alignment=f"data/{database}/nextclade.aligned.old.fasta",
        translations=[
            f"data/{database}/nextclade.translation_{gene}.old.fasta"
            for gene in GENE_LIST
        ],
    shell:
        """
        touch {output}
        """


# Only include rules to fetch from S3 if S3 config params are provided
if config.get("s3_dst") and config.get("s3_src"):

    # Set ruleorder since these rules have the same output
    # Allows us to only download the NextClade cache from S3 only if the
    # S3 parameters are provided in the config.
    ruleorder: download_nextclade_tsv_from_s3 > create_empty_nextclade_info
    ruleorder: download_previous_alignment_from_s3 > create_empty_nextclade_aligned

    rule download_nextclade_tsv_from_s3:
        """
        If there's a .renew touchfile, do not use the cache
        """
        params:
            dst_source=config["s3_dst"] + "/nextclade{reference}.tsv.zst",
            src_source=config["s3_src"] + "/nextclade{reference}.tsv.zst",
            dst_rerun_touchfile=config["s3_dst"] + "/nextclade{reference}.tsv.zst.renew",
            src_rerun_touchfile=config["s3_dst"] + "/nextclade{reference}.tsv.zst.renew",
            lines=config.get("subsample", {}).get("nextclade", 0),
        output:
            nextclade=f"data/{database}/nextclade{{reference}}_old.tsv",
        shell:
            """
            ./bin/download-from-s3 {params.dst_rerun_touchfile} {output.nextclade} 0 ||  \
            ./bin/download-from-s3 {params.src_rerun_touchfile} {output.nextclade} 0 ||  \
            ./bin/download-from-s3 {params.dst_source} {output.nextclade} {params.lines} ||  \
            ./bin/download-from-s3 {params.src_source} {output.nextclade} {params.lines} ||  \
            touch {output.nextclade}
            """

    rule download_previous_alignment_from_s3:
        ## NOTE two potential bugs with this implementation:
        ## (1) race condition. This file may be updated on the remote after download_nextclade has run but before this rule
        ## (2) we may get `download_nextclade` and `download_previous_alignment` from different s3 buckets
        params:
            dst_source=config["s3_dst"] + "/{seqtype}.fasta.zst",
            src_source=config["s3_src"] + "/{seqtype}.fasta.zst",
            dst_rerun_touchfile=config["s3_dst"] + "/nextclade.tsv.zst.renew",
            src_rerun_touchfile=config["s3_dst"] + "/nextclade.tsv.zst.renew",
            lines=config.get("subsample", {}).get("nextclade", 0),
        output:
            alignment=temp(f"data/{database}/nextclade.{{seqtype}}.old.fasta"),
        shell:
            """
            ./bin/download-from-s3 {params.dst_rerun_touchfile} {output.alignment} 0 ||  \
            ./bin/download-from-s3 {params.src_rerun_touchfile} {output.alignment} 0 ||  \
            ./bin/download-from-s3 {params.dst_source} {output.alignment} {params.lines} ||  \
            ./bin/download-from-s3 {params.src_source} {output.alignment} {params.lines} ||  \
            touch {output.alignment}
            """


rule get_sequences_without_nextclade_annotations:
    """Find sequences in FASTA which don't have clades assigned yet"""
    input:
        fasta=f"data/{database}/sequences.fasta",
        nextclade=f"data/{database}/nextclade{{reference}}_old.tsv",
    output:
        fasta=f"data/{database}/nextclade{{reference}}.sequences.fasta",
    benchmark:
        f"benchmarks/get_sequences_without_nextclade_annotations_{database}{{reference}}.txt"
    shell:
        """
        if [[ -s {input.nextclade} ]]; then
            ./bin/filter-fasta \
                --input_fasta={input.fasta} \
                --input_tsv={input.nextclade} \
                --output_fasta={output.fasta}
        else
            ln {input.fasta} {output.fasta}
        fi
        echo "[ INFO] Number of {wildcards.reference} sequences to run Nextclade on: $(grep -c '^>' {output.fasta})"
        """


rule download_nextclade_executable:
    """Download Nextclade"""
    output:
        nextclade="nextclade",
    shell:
        """
        if [ "$(uname)" = "Darwin" ]; then
            curl -fsSL "https://github.com/nextstrain/nextclade/releases/latest/download/nextclade-x86_64-apple-darwin" -o "nextclade"
        else
            curl -fsSL "https://github.com/nextstrain/nextclade/releases/latest/download/nextclade-x86_64-unknown-linux-gnu" -o "nextclade"
        fi
        chmod +x nextclade

        if ! command -v ./nextclade &>/dev/null; then
            echo "[ERROR] Nextclade executable not found"
            exit 1
        fi

        NEXTCLADE_VERSION="$(./nextclade --version)"
        echo "[ INFO] Nextclade version: $NEXTCLADE_VERSION" 
        """


rule download_nextclade_dataset:
    """Download Nextclade dataset"""
    input:
        "nextclade",
    output:
        dataset="data/nextclade_data/{dataset_name}.zip",
    shell:
        """
        ./nextclade dataset get --name="{wildcards.dataset_name}" --output-zip={output.dataset} --verbose
        """


rule run_nextclade:
    message:
        """
        Runs nextclade on sequences which were not in the previously cached nextclade run.
        This alignes sequences, assigns clades and calculates some of the other useful
        metrics which will ultimately end up in metadata.tsv.
        """
    input:
        nextclade="nextclade",
        dataset=lambda w: f"data/nextclade_data/sars-cov-2{w.reference.replace('_','-')}.zip",
        sequences=f"data/{database}/nextclade{{reference}}.sequences.fasta",
    params:
        genes=GENES_SPACE_DELIMITED,
        translation_arg=lambda w: (
            # Nextclade takes a filename template in which it replaces {gene}
            # itself, so we want to pass thru {gene} literally to it and make
            # sure it isn't interpretted by the shell as a glob.  We shellquote
            # here instead of with :q below because we don't want to pass an
            # empty string argument when this param is empty.
            shellquote(f"--output-translations=data/{database}/nextclade{w.reference}.translation_{{gene}}.upd.fasta")
            if w.reference == ""
            else ""
        ),
    output:
        info=f"data/{database}/nextclade{{reference}}_new_raw.tsv",
        alignment=temp(f"data/{database}/nextclade{{reference}}.aligned.upd.fasta"),
        translations=[temp(f"data/{database}/nextclade{{reference}}.translation_{gene}.upd.fasta") for gene in GENE_LIST],
    shell:
        """
        if [[ -s {input.sequences} ]]; then
            ./nextclade run \
            {input.sequences}\
            --input-dataset={input.dataset} \
            --output-tsv={output.info} \
            --genes {params.genes} \
            {params.translation_arg} \
            --output-fasta={output.alignment}
        else
            touch {output.info} {output.alignment} {output.translations}
            echo "[ INFO] Skipping Nextclade run as there are no new sequences"
        fi
        """


rule nextclade_tsv_concat_versions:
    input:
        tsv=f"data/{database}/nextclade{{reference}}_new_raw.tsv",
        dataset=lambda w: f"data/nextclade_data/sars-cov-2{w.reference.replace('_','-')}.zip",
    output:
        tsv=f"data/{database}/nextclade{{reference}}_new.tsv",
    shell:
        """
        if [ -s {input.tsv} ]; then
            # Get version numbers
            nextclade_version="$(./nextclade --version)"
            dataset_version="$(unzip -p {input.dataset} tag.json | jq -r '.tag')"
            timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

            # Combine input file with version numbers and write to output
            printf "%s\tnextclade_version\tdataset_version\trun_timestamp\n" \
                "$(head -n 1 {input.tsv})" \
                > {output.tsv}

            tail -n +2 {input.tsv} | \
            awk -v v1="$nextclade_version" \
                -v v2="$dataset_version" \
                -v v3="$timestamp" \
                -v OFS='\t' '{{print $0, v1, v2, v3}}' \
                >> {output.tsv}
        else
            cp {input.tsv} {output.tsv}
        fi
        """


rule nextclade_info:
    message:
        """
        Generates nextclade info TSV for all sequences (new + old)
        """
    input:
        old_info=f"data/{database}/nextclade{{reference}}_old.tsv",
        new_info=rules.nextclade_tsv_concat_versions.output.tsv,
    output:
        nextclade_info=f"data/{database}/nextclade{{reference}}.tsv",
    shell:
        """
        tsv-append -H {input.old_info} {input.new_info} \
        | tsv-uniq -H -f seqName > {output.nextclade_info}
        """


rule combine_alignments:
    """
    Generating full alignment by combining newly aligned sequences with previous (cached) alignment
    """
    input:
        old_alignment=f"data/{database}/nextclade.{{seqtype}}.old.fasta",
        new_alignment=f"data/{database}/nextclade.{{seqtype}}.upd.fasta",
    output:
        alignment=f"data/{database}/{{seqtype}}.fasta",
    params:
        keep_temp=config.get("keep_temp", "false"),
    shell:
        """
        if [[ -s {input.old_alignment} ]]; then
            if [[ "{params.keep_temp}" == "True" ]]; then
                cp {input.old_alignment} {output.alignment}
            else
                mv {input.old_alignment} {output.alignment}
            fi
            cat {input.new_alignment} >> {output.alignment}
        elif [[ "{params.keep_temp}" == "True" ]]; then
            cp {input.new_alignment} {output.alignment}
        else
            mv {input.new_alignment} {output.alignment}
        fi
        """


rule generate_metadata:
    input:
        nextclade_tsv=f"data/{database}/nextclade.tsv",
        nextclade_21L_tsv=f"data/{database}/nextclade_21L.tsv",
        existing_metadata=f"data/{database}/metadata_transformed.tsv",
        clade_legacy_mapping="defaults/clade-legacy-mapping.yml",
    output:
        metadata=f"data/{database}/metadata.tsv",
    benchmark:
        f"benchmarks/generate_metadata_{database}.txt"
    shell:
        """
        ./bin/join-metadata-and-clades \
            --metadata {input.existing_metadata} \
            --nextclade-tsv {input.nextclade_tsv} \
            --nextclade-21L-tsv {input.nextclade_21L_tsv} \
            --clade-legacy-mapping {input.clade_legacy_mapping} \
            -o {output.metadata}
        """
