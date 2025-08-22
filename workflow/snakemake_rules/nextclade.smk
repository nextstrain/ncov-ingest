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


wildcard_constraints:
    reference="|_21L",
    seqtype="aligned|translation_[^.]+",


rule create_empty_nextclade_info:
    """Creating empty NextClade info cache file"""
    output:
        touch(f"data/{database}/nextclade{{reference}}_old.tsv"),
    benchmark:
        f"benchmarks/create_empty_nextclade_info_{database}{{reference}}.txt"


rule create_empty_nextclade_aligned:
    """Creating empty NextClade aligned cache file"""
    output:
        touch(f"data/{database}/nextclade.aligned.old.fasta"),
        *[
            touch(f"data/{database}/nextclade.translation_{gene}.old.fasta")
            for gene in GENE_LIST
        ],
    benchmark:
        f"benchmarks/create_empty_nextclade_aligned_{database}.txt"


# Only include rules to fetch from S3 if S3 config params are provided
if config.get("s3_dst") and config.get("s3_src"):

    # Set ruleorder since these rules have the same output
    # Allows us to only download the NextClade cache from S3 only if the
    # S3 parameters are provided in the config.
    ruleorder: download_nextclade_tsv_from_s3 > create_empty_nextclade_info
    ruleorder: download_previous_alignment_from_s3 > create_empty_nextclade_aligned


    rule use_nextclade_cache:
        input:
            nextclade="data/nextclade",
            nextclade_dataset=lambda w: f"data/nextclade_data/sars-cov-2{w.reference.replace('_','-')}.zip",
        params:
            dst_source=config["s3_dst"],
            src_source=config["s3_src"],
        output:
            use_nextclade_cache=f"data/{database}/use_nextclade_cache{{reference}}.txt",
        shell:
            """
            ./bin/use-nextclade-cache \
                {params.dst_source:q} \
                {params.src_source:q} \
                {input.nextclade:q} \
                {input.nextclade_dataset:q} \
                {wildcards.reference:q} \
                > {output.use_nextclade_cache}
            """


    rule download_nextclade_tsv_from_s3:
        """
        If there's a .renew touchfile, do not use the cache
        """
        input:
            use_nextclade_cache=f"data/{database}/use_nextclade_cache{{reference}}.txt",
        params:
            dst_source=config["s3_dst"] + "/nextclade{reference}.tsv.zst",
            src_source=config["s3_src"] + "/nextclade{reference}.tsv.zst",
            lines=config.get("subsample", {}).get("nextclade", 0),
        output:
            nextclade=f"data/{database}/nextclade{{reference}}_old.tsv",
        benchmark:
            f"benchmarks/download_nextclade_tsv_from_s3_{database}{{reference}}.txt"
        shell:
            """
            use_nextclade_cache=$(cat {input.use_nextclade_cache})

            if [[ "$use_nextclade_cache" == 'true' ]]; then
                echo "[INFO] Downloading cached nextclade{wildcards.reference}.tsv.zst"
                ./vendored/download-from-s3 {params.dst_source} {output.nextclade} {params.lines} ||  \
                ./vendored/download-from-s3 {params.src_source} {output.nextclade} {params.lines}
            else
                echo "[INFO] Ignoring cached nextclade{wildcards.reference}.tsv.zst"
                touch {output.nextclade}
            fi
            """

    rule download_previous_alignment_from_s3:
        ## NOTE two potential bugs with this implementation:
        ## (1) race condition. This file may be updated on the remote after download_nextclade has run but before this rule
        ## (2) we may get `download_nextclade` and `download_previous_alignment` from different s3 buckets
        input:
            use_nextclade_cache=f"data/{database}/use_nextclade_cache.txt",
        params:
            dst_source=config["s3_dst"] + "/{seqtype}.fasta.zst",
            src_source=config["s3_src"] + "/{seqtype}.fasta.zst",
            lines=config.get("subsample", {}).get("nextclade", 0),
        output:
            alignment=temp(f"data/{database}/nextclade.{{seqtype}}.old.fasta"),
        benchmark:
            f"benchmarks/download_previous_alignment_from_s3_{database}{{seqtype}}.txt"
        shell:
            """
            use_nextclade_cache=$(cat {input.use_nextclade_cache})

            if [[ "$use_nextclade_cache" == 'true' ]]; then
                echo "[INFO] Downloading cached Nextclade {wildcards.seqtype}.fasta.zst"
                ./vendored/download-from-s3 {params.dst_source} {output.alignment} {params.lines} ||  \
                ./vendored/download-from-s3 {params.src_source} {output.alignment} {params.lines}
            else
                echo "[INFO] Ignoring cached Nextclade {wildcards.seqtype}.fasta.zst"
                touch {output.alignment}
            fi
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
        nextclade="data/nextclade",
    benchmark:
        f"benchmarks/download_nextclade_executable_{database}.txt"
    shell:
        """
        if [ "$(uname)" = "Darwin" ]; then
            curl -fsSL "https://github.com/nextstrain/nextclade/releases/latest/download/nextclade-x86_64-apple-darwin" -o {output.nextclade:q}

        else
            curl -fsSL "https://github.com/nextstrain/nextclade/releases/latest/download/nextclade-x86_64-unknown-linux-gnu" -o {output.nextclade:q}
        fi
        chmod +x {output.nextclade:q}

        if ! command -v {output.nextclade:q} &>/dev/null; then
            echo "[ERROR] Nextclade executable not found"
            exit 1
        fi

        NEXTCLADE_VERSION="$({output.nextclade:q} --version)"
        echo "[ INFO] Nextclade version: $NEXTCLADE_VERSION"
        """


rule download_nextclade_dataset:
    """Download Nextclade dataset"""
    input:
        nextclade="data/nextclade",
    output:
        dataset="data/nextclade_data/{dataset_name}.zip",
    benchmark:
        f"benchmarks/download_nextclade_dataset_{database}_{{dataset_name}}.txt"
    shell:
        """
        {input.nextclade:q} dataset get --name="{wildcards.dataset_name}" --output-zip={output.dataset} --verbose
        """


rule run_wuhan_nextclade:
    """
    Runs nextclade on sequences which were not in the previously cached nextclade run.
    This alignes sequences, assigns clades and calculates some of the other useful
    metrics which will ultimately end up in metadata.tsv.
    """
    input:
        nextclade_path="data/nextclade",
        dataset="data/nextclade_data/sars-cov-2.zip",
        sequences=f"data/{database}/nextclade.sequences.fasta",
    params:
        translation_arg=lambda w: (
            f"--output-translations=data/{database}/nextclade.translation_{{cds}}.upd.fasta"
        ),
    output:
        info=f"data/{database}/nextclade_new.tsv",
        alignment=temp(f"data/{database}/nextclade.aligned.upd.fasta"),
        translations=[
            temp(f"data/{database}/nextclade.translation_{gene}.upd.fasta")
            for gene in GENE_LIST
        ],
    threads:
        workflow.cores * 0.5
    benchmark:
        f"benchmarks/run_wuhan_nextclade_{database}.txt"
    shell:
        """
        ./{input.nextclade_path} run \
        -j {threads} \
        {input.sequences}\
        --input-dataset={input.dataset} \
        --output-tsv={output.info} \
        {params.translation_arg} \
        --output-fasta={output.alignment}
        """


rule run_21L_nextclade:
    """
    Like wuhan nextclade, but TSV only, no alignments output
    """
    input:
        nextclade_path="data/nextclade",
        dataset=lambda w: f"data/nextclade_data/sars-cov-2-21L.zip",
        sequences=f"data/{database}/nextclade_21L.sequences.fasta",
    output:
        info=f"data/{database}/nextclade_21L_new.tsv",
    threads:
        workflow.cores * 0.5
    benchmark:
        f"benchmarks/run_21L_nextclade_{database}.txt"
    shell:
        """
        ./{input.nextclade_path} run \
        -j {threads} \
        {input.sequences} \
        --input-dataset={input.dataset} \
        --output-tsv={output.info} \
        """


rule nextclade_info:
    """
    Generates nextclade info TSV for all sequences (new + old)
    """
    input:
        old_info=f"data/{database}/nextclade{{reference}}_old.tsv",
        new_info=f"data/{database}/nextclade{{reference}}_new.tsv",
    output:
        nextclade_info=f"data/{database}/nextclade{{reference}}.tsv",
    benchmark:
        f"benchmarks/nextclade_info_{database}{{reference}}.txt"
    shell:
        """
        tsv-append -H {input.old_info} {input.new_info} \
        | tsv-uniq -H -f seqName > {output.nextclade_info}
        """


rule nextclade_version_json:
    """
    Generates a version JSON for the Nextclade TSV.
    """
    input:
        nextclade_path="data/nextclade",
        nextclade_dataset=lambda w: f"data/nextclade_data/sars-cov-2{w.reference.replace('_','-')}.zip",
        nextclade_tsv=f"data/{database}/nextclade{{reference}}.tsv",
    output:
        nextclade_version_json=f"data/{database}/nextclade{{reference}}_version.json",
    shell:
        """
        ./bin/generate-nextclade-version-json \
            {input.nextclade_path} \
            {input.nextclade_dataset} \
            {input.nextclade_tsv} \
            > {output.nextclade_version_json}
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
    benchmark:
        f"benchmarks/combine_alignments_{database}{{seqtype}}.txt"
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


rule metadata_version_json:
    """
    Generates the metadata version JSON by adding the metadata TSV sha256sum
    to the Nextclade version JSON.

    TODO: Merge the 21L Nextclade version JSON to track data provenence for
    specific columns
    """
    input:
        metadata=f"data/{database}/metadata.tsv",
        nextclade_version_json=f"data/{database}/nextclade_version.json",
    output:
        metadata_version_json=f"data/{database}/metadata_version.json",
    shell:
        """
        metadata_tsv_sha256sum="$(./vendored/sha256sum < {input.metadata})"

        cat {input.nextclade_version_json} \
            | jq -c --arg METADATA_TSV_SHA256SUM "$metadata_tsv_sha256sum" \
                '.metadata_tsv_sha256sum = $METADATA_TSV_SHA256SUM' \
                > {output.metadata_version_json}
        """
