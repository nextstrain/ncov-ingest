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
    message: "Triggering nextstrain/ncov rebuild action (via repository dispatch)"
    input:
        metadata_upload = f"data/{database}/metadata.tsv.gz.upload",
        fasta_upload = f"data/{database}/sequences.fasta.xz.upload",
    output:
        touch(f"data/{database}/trigger-rebuild.done")
    params:
        dispatch_type = f"{database}/rebuild"
    shell:
        """
        ./vendored/trigger-on-new-data \
            nextstrain/ncov \
            {params.dispatch_type} \
            {input.metadata_upload} \
            {input.fasta_upload}
        """

rule trigger_counts_pipeline:
    message: "Triggering nextstrain/counts clade counts action (via repository dispatch)"
    input:
        f"data/{database}/upload.done"
    output:
        touch(f"data/{database}/trigger-counts.done")
    params:
        dispatch_type = f"{database}/clade-counts"
    shell:
        """
        ./vendored/trigger nextstrain/forecasts-ncov {params.dispatch_type}
        """
