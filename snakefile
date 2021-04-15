
envvars:
    "S3_SRC",
    "GITHUB_REF",
    "GISAID_API_ENDPOINT",
    "GISAID_USERNAME_AND_PASSWORD",
    "SLACK_TOKEN",
    "FETCH"


configfile: "snake_config.yaml"

wildcard_constraints:
    database = "gisaid|genbank"


localrules: all_then_clean , fetch , notify_and_upload


## defining some of the behaviour depending on 
## which git branch we are

GIT_BRANCH = ""
SILENT = ""
S3_DST = ''

github_ref = os.environ[ "GITHUB_REF" ]
if github_ref == "refs/heads/master" :
    GIT_BRANCH = "master"
    S3_DST = os.environ['S3_SRC']

elif github_ref.startswith('refs/heads/') :
    SILENT = 'yes'
    GIT_BRANCH = github_ref[ len('refs/heads/') : ]
    S3_DST = os.environ['S3_SRC'] + "/branch/" + GIT_BRANCH

elif github_ref=='':
    SILENT = 'yes'
    S3_DST = os.environ['S3_SRC'] + "/tmp"
else:
    print("skipping ingest for ref",github_ref)
    exit(0)

print( "S3_SRC is" , os.environ['S3_SRC'] , file=sys.stderr )
print( "S3_DST is" , S3_DST , file=sys.stderr )

## defining the slack channel that will be notified:

os.environ["SLACK_CHANNELS"] = lambda wildcards : config['slack_channel'][wildcards.database]



## target rule all 
rule all_then_clean:
    input:
        "notify_and_upload.gisaid.mock_output.txt",
        "notify_and_upload.genbank.mock_output.txt"
    shell:
        ".bin/clean"


## target rule gisaid
rule gisaid_then_clean:
    input:
        "notify_and_upload.gisaid.mock_output.txt",
    shell:
        ".bin/clean"

## target rule genbank
rule genbank_then_clean:
    input:
        "notify_and_upload.genbank.mock_output.txt",
    shell:
        ".bin/clean"



rule fetch:
    output:
        "data/{database}.ndjson"
    params:
        s3_dst=S3_DST,
        database = lambda wildcards : wildcards.database
    shell:
           '''
            if [[ "$FETCH" == 1 ]]; then
              ./bin/fetch-from-{params.database} > {output}
              if [[ "$branch" == master ]]; then

                dst=$S3_SRC/{params.database}.ndjson.gz

                src_record_count="$(wc -l < "$src")"
                dst_record_count="$(wc -l < <(aws s3 cp --no-progress "$dst" - | gunzip -cfq))"
                added_records="$(( src_record_count - dst_record_count ))"

                msg=""

                if [[ $added_records -gt 0 ]]; then
                    msg="📈 New nCoV records (n=$added_records) found on {params.database}."
                elif [[ $added_records -lt 0 ]]; then
                    msg="WARNING: the new version of {params.database} has fewer records‽"
                
                else
                    msg="📈 New nCoV records (n=$added_records) found on {params.database}."
                fi

                ./bin/notify-slack $msg $SLACK_TOKEN $SLACK_CHANNELS 

              fi
              ./bin/upload-to-s3 --quiet {output} "{params.s3_dst}/{params.database}.ndjson.gz"
            else
              aws s3 cp --no-progress "{params.s3_dst}/{params.database}.ndjson.gz" - | gunzip -cfq > {output}
            fi
           '''


rule transform_gisaid:
    input:
        "data/gisaid.ndjson"
    output:
        metadata="data/gisaid/metadata.noClade.tsv",
        fasta="data/gisaid/sequences.fasta",
        additional_info="data/gisaid/additional_info.tsv",
        flagged_annotation="data/gisaid/transform-log.txt"
    shell:
        """
          ./bin/transform-gisaid {input} \
            --output-metadata {output.metadata} \
            --output-fasta {output.fasta} \
            --output-unix-newline \
            --output-additional-info {output.additional_info} > {output.flagged_annotation}
        """

rule transform_genbank:
    input:
        "data/genbank.ndjson"
    output:
        metadata="data/genbank/metadata.noClade.tsv",
        fasta="data/genbank/sequences.fasta",
        problem="data/genbank/problem_data.tsv",
        flagged_annotation="data/genbank/transform-log.txt",
        additional_info="data/genbank/additional_info.tsv",
    shell :
        '''
        ./bin/transform-genbank {input} \
            --output-metadata {output.metadata} \
            --output-fasta {output.sequences} \
            --problem-data {output.problem} > {output.flagged_annotation}

        touch {output.additional_info}
        '''




rule dowload_old_clades :
    output:
        "data/{database}/nextclade.old.tsv"
    params:
        dst_source=S3_DST+'/nextclade.tsv.gz',
        src_source='$S3_SRC/nextclade.tsv.gz',
    shell:
        '( aws s3 cp --no-progress "{params.dst_source}" - || aws s3 cp --no-progress "{params.src_source}" -) | gunzip -cfq > {output} '


rule filter_fasta :
    input:
        fasta = lambda wildcards : "data/"+wildcards.database+"/sequences.fasta",
        tsv = rules.dowload_old_clades.output
    output:
        "data/{database}/nextclade.sequences.fasta"
    shell: 
        "./bin/filter-fasta --input_fasta=input.fasta --input_tsv=input.tsv --output_fasta={output}" 

rule run_nextclade :
    input:
        rules.filter_fasta.output
    output:
        "data/{database}/nextclade.new.tsv"
    shell:
        "./bin/run-nextclade {input} {output}"

rule join_clades :
    input:
        old=rules.dowload_old_clades.output ,
        new=rules.run_nextclade.output
    output:
        "data/{database}/nextclade.tsv"
    shell:
        "./bin/join-rows {input.new} {input.old} -o {output}"


rule join_metadata_and_clades :
    input:
        clades = rules.join_clades.output ,
        meta = lambda wildcards : "data/"+wildcards.database+"/metadata.noClade.tsv"
    output:
        "data/{database}/metadata.tsv"
    shell:
        "./bin/join-metadata-and-clades {input.meta} {input.clades} -o {output}"

rule flag_metadata :
    input :
        rules.join_metadata_and_clades.output
    output :
        "data/{database}/flagged_metadata.txt"
    shell:
        "./bin/flag-metadata {input} > {output}"

rule check_locations :
    input :
        rules.join_metadata_and_clades.output
    params:
        idcolumn=lambda wildcards : config['idcolumn'][wildcards.database]
    output :
        "data/{database}/location_hierarchy.tsv"
    shell:
        "./bin/check-locations {input} {output} {params.idcolumn}"


rule notify_and_upload:
    input :
        sequences = lambda wildcards : "data/"+wildcards.database+"/sequences.fasta",
        metadata = lambda wildcards : "data/"+wildcards.database+"/metadata.tsv",
        nextclade = lambda wildcards : "data/"+wildcards.database+"/nextclade.tsv",
        additional_info = lambda wildcards : "data/"+wildcards.database+"/additional_info.tsv",
        flagged_metadata = lambda wildcards : "data/"+wildcards.database+"/flagged_metadata.txt",
        flagged_annotation = lambda wildcards : "data/"+wildcards.database+"/transform-log.txt",
        location_hierarchy = lambda wildcards : "data/"+wildcards.database+"/location_hierarchy.tsv"
    output :
        "notify_and_upload.{database}.mock_output.txt"
    params :
        idcolumn=lambda wildcards : config['idcolumn'][wildcards.database],
        destination_metadata = lambda wildcards : "$S3_SRC/"+ wildcards.database +"_metadata.tsv.gz",
        destination_additional_info = lambda wildcards : "$S3_SRC/"+ wildcards.database +"_additional_info.tsv.gz",
        destination_flagged_metadata = lambda wildcards : "$S3_SRC/"+ wildcards.database +"_flagged_metadata.txt.gz",
        destination_sequences = lambda wildcards : "$S3_SRC/"+ wildcards.database +"_sequences.fasta.gz",
        destination_nextclade = "$S3_SRC/nextclade.tsv.gz",
        quiet = (SILENT=='yes')
    run :
        if GIT_BRANCH == "master" :
            # upload flagged annotations
            shell(f"""
                # upload flagged annotations
                ./bin/notify-slack --upload "flagged-annotations" $SLACK_TOKEN $SLACK_CHANNELS < {input.flagged_annotation}
            """)

            # "Notifying Slack about metadata change."
            shell(f"""
                # notify and upload metadata change
                
                dst_local="$(mktemp -t metadata-XXXXXX.tsv)"
                diff="$(mktemp -t metadata-changes-XXXXXX)"
                additions="$(mktemp -t metadata-additions-XXXXXX)"
                trap "rm -f '$dst_local' '$diff' '$additions'" EXIT

                ./bin/compute-metadata-change {input.metadata} "{params.destination_metadata}" {params.idcolumn} $dst_local $diff $additions


                # csv-diff outputs two newlines which -n ignores but -s does not
                if [[ -n "$(< "$diff")" ]]; then
                    # "Notifying Slack about metadata change."
                    ./bin/notify-slack --upload "metadata-changes.txt" $SLACK_TOKEN $SLACK_CHANNELS < "$diff"
                else
                    echo "No metadata change."
                fi
                # checking additions
                if [[ -s "$additions" ]]; then
                    # "Notifying Slack about metadata additions."
                    ./bin/notify-slack --upload "metadata-additions.tsv" $SLACK_TOKEN $SLACK_CHANNELS < "$additions"
                 
                    if [[ "{params.idcolumn}" == "gisaid_epi_isl" ]]; then
                        ./bin/notify-users-on-new-locations "$additions" --slack-token $SLACK_TOKEN --slack-channel $SLACK_CHANNELS
                    fi
                fi
            """)
            
            # "Notifying Slack about location hierarchy additions."
            shell(f"""

                diff="$(mktemp -t location-hierarchy-changes-XXXXXX)"
                trap "rm -f '$diff'" EXIT

                ./bin/compute-location-hierarchy-addition {input.location_hierarchy} source-data/location_hierarchy.tsv $diff

                if [[ -s "$diff" ]]; then
                    # "Notifying Slack about location hierarchy additions."
                    message=":world_map: $(wc -l < "$diff") new location hierarchies. "
                    message+="Note that these are case-sensitive. Please review these "
                    message+="hierarchies and either add them to "
                    message+="_./source-data/location_hierarchy.tsv_ or create new annotations "
                    message+="to correct them."
                
                    ./bin/notify-slack "$message" $SLACK_TOKEN $SLACK_CHANNELS
                    ./bin/notify-slack --upload "location-hierarchy-additions.tsv" $SLACK_TOKEN $SLACK_CHANNELS < "$diff"
                fi

            """)
            
            # "Notifying Slack about additional info change."
            shell(f"""

                diff="$(mktemp -t location-hierarchy-changes-XXXXXX)"
                trap "rm -f '$diff'" EXIT

                ./bin/compute-additional-info-change {input.additional_info} "{params.destination_additional_info}" $diff

                if [[ -n "$diff" ]]; then
                    # "Notifying Slack about additional info change."
                    ./bin/notify-slack --upload "additional-info-changes.txt" $SLACK_TOKEN $SLACK_CHANNELS < "$diff"
                else
                    echo "No additional info change."
                fi

            """)

            # "Notifying Slack about flagged metadata additions."
            shell(f"""

                dst_local="$(mktemp -t flagged-metadata-XXXXXX.txt)"
                diff="$(mktemp -t flagged-metadata-additions-XXXXXX)"
                trap "rm -f '$dst_local' '$diff'" EXIT

                ./bin/compute-flagged-metadata-change {input.flagged_metadata} "{params.destination_flagged_metadata}" $dst_local $diff
                if [[ -s "$diff" ]]; then
                    # "Notifying Slack about flagged metadata additions."
                    ./bin/notify-slack ":waving_black_flag: Newly flagged metadata" $SLACK_TOKEN $SLACK_CHANNELS
                    ./bin/notify-slack --upload "flagged-metadata-additions.txt" $SLACK_TOKEN $SLACK_CHANNELS < "$diff"
                else
                    echo "No flagged metadata additions."
                fi

            """)

            # "Notifying Slack about problem data."
            shell(f"""
            
                if [[ -s "data/genbank/problem_data.tsv" ]]; then
                    # "Notifying Slack about problem data."
                    ./bin/notify-slack --upload "genbank-problem-data.tsv" $SLACK_TOKEN $SLACK_CHANNELS < "data/genbank/problem_data.tsv"
                fi

            """)

        shell(f"""
            ./bin/upload-to-s3 {params.quiet} {input.metadata} "{params.destination_metadata}"
            ./bin/upload-to-s3 {params.quiet} {input.sequences} "{params.destination_sequences}"
            ./bin/upload-to-s3 {params.quiet} {input.nextclade} "{params.destination_nextclade}"
   
            ./bin/upload-to-s3 {params.quiet} {input.additional_info} "{params.destination_additional_info}"
            ./bin/upload-to-s3 {params.quiet} {input.flagged_metadata} "{params.destination_flagged_metadata}"
        """)
        if not params.quiet :
            shell("""
                for dst in  "{params.destination_metadata}" "{params.destination_sequences}" "{params.destination_nextclade}" "{params.destination_additional_info}" "{params.destination_flagged_metadata}"
                do
                 ./bin/notify-slack "Updated $dst available."  $SLACK_TOKEN $SLACK_CHANNELS
                done
            """)

        shell(f"""   
            touch {output}
        """)