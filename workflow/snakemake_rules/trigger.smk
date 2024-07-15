"""
This part of the workflow handles various GitHub Action triggers.
Depends on the main Snakefile to define the variable `database`, which is NOT a wildcard.

Expects the input file:
    "data/{database}/upload.done"

Produces the output files:
    "data/{database}/trigger-rebuild.done"
    "data/{database}/trigger-counts.done"
These output files are empty flag files to force Snakemake to run the trigger rules.
"""

rule trigger_rebuild_pipeline:
    """Triggering nextstrain/ncov rebuild action (via repository dispatch)"""
    input:
        metadata_upload = f"data/{database}/metadata.tsv.zst.upload",
        fasta_upload = f"data/{database}/aligned.fasta.zst.upload",
    output:
        touch(f"data/{database}/trigger-rebuild.done")
    benchmark:
        f"benchmarks/trigger_rebuild_pipeline_{database}.txt"
    params:
        dispatch_type = f"{database}/rebuild"
    retries: 5
    shell:
        """
        ./vendored/trigger-on-new-data \
            nextstrain/ncov \
            {params.dispatch_type} \
            {input.metadata_upload} \
            {input.fasta_upload}
        """

rule trigger_counts_pipeline:
    """Triggering nextstrain/counts clade counts action (via repository dispatch)"""
    input:
        f"data/{database}/upload.done"
    output:
        touch(f"data/{database}/trigger-counts.done")
    benchmark:
        f"benchmarks/trigger_counts_pipeline_{database}.txt"
    params:
        dispatch_type = f"{database}/clade-counts"
    retries: 5
    shell:
        """
        ./vendored/trigger nextstrain/forecasts-ncov {params.dispatch_type}
        """
