"""
Creates partitioned datasets:
- by year_month
- by clade
- by continent
"""


rule metadata_by_year_month:
    input:
        "data/{database}/metadata.tsv",
    output:
        "data/{database}/metadata_year-month_{year}-{month}.tsv",
    shell:
        """
        tsv-filter -H --istr-in-fld "date:{wildcards.year}-{wildcards.month}" {input} > {output}
        """


rule metadata_by_clade:
    input:
        "data/{database}/metadata.tsv",
    output:
        "data/{database}/metadata_clade_{clade}.tsv",
    shell:
        """
        tsv-filter -H --istr-in-fld "Nextstrain_clade:{wildcards.clade}" {input} > {output}
        """


rule metadata_by_continent:
    input:
        "data/{database}/metadata.tsv",
    output:
        "data/{database}/metadata_region_{continent}.tsv",
    shell:
        """
        tsv-filter -H --istr-eq "region:{wildcards.continent}" {input} > {output}
        """

rule sequences_by_metadata:
    input:
        sequences="data/{database}/sequences.fasta",
        metadata="data/{database}/metadata_{partition}.tsv",
    output:
        sequences="data/{database}/sequences_{partition}.fasta",
        strains=temp("data/{database}/strains_{partition}.txt"),
    shell:
        """
        tsv-select -H -f strain {input.metadata} > {output.strains}
        seqkit grep -f {output.strains} {input.sequences} > {output.sequences}
        """
