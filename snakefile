


configfile: "snake_config.yaml"

wildcard_constraints:
    database = "gisaid|genbank"


localrules: all_then_clean, gisaid_then_clean , genbank_then_clean,
            ingest_genbank, ingest_gisaid, download_old_clades,
            fetch, notify_and_upload, get_nextclade_inputs, additional_info_changes,
            new_flagged_metadata, additional_info_changes, location_hierarchy_additions,
            metadata_change, upload_and_notify_generic, upload_ndjson, upload_file, additional_info_changes


# we want to check if some environment variable exists.
#since the envvars: directive only works for later versions of snakemake, we have to do this "nmanually":
requiredEnvironmentVariables = [ "GITHUB_REF"]
absentRequiredEnvironmentVariables = [v for v in requiredEnvironmentVariables if not v in os.environ ]
if len( absentRequiredEnvironmentVariables )>0:
    raise Exception("The following environment variables are requested by the workflow but undefined. Please make sure that they are correctly defined before running Snakemake:\n" + '\n'.join(absentRequiredEnvironmentVariables) )



## defining some of the behaviour depending on
## which git branch we are

github_ref = os.environ[ "GITHUB_REF" ]
if github_ref == "refs/heads/master" :
    BRANCH_SUFFIX = ""
    NOTIFY = True
elif github_ref.startswith('refs/heads/') :
    BRANCH_SUFFIX = "/branch/" + github_ref[ len('refs/heads/') : ]
    NOTIFY = False
else:
    print("skipping ingest for ref",github_ref)
    exit(0)

## defining some environment variables for gisaid fetching:
if "gisaid_endpoint" in config:
    os.environ["GISAID_API_ENDPOINT"] = config['gisaid_endpoint']
if "gisaid_login" in config :
    os.environ["GISAID_USERNAME_AND_PASSWORD"] = config['gisaid_login']

## defining some environment variables for slack notifications:
if "slack_token" in config:
    os.environ["SLACK_TOKEN"] = config['slack_token']

def _get_slack_channel(w):
    if NOTIFY:
        return config['slack_channel'][w.database]
    else:
        "none"

def _get_S3_DST(w):
    if w.database=='gisaid':
        return "s3://nextstrain-ncov-private" + BRANCH_SUFFIX
    elif w.database=='genbank':
        return "s3://nextstrain-data/files/ncov/open" + BRANCH_SUFFIX
    else:
        ValueError(f"get_S3_DST: database {w.database} is unknown.")

def _get_S3_SRC(w):
    if w.database=='gisaid':
        return "s3://nextstrain-ncov-private"
    elif w.database=='genbank':
        return "s3://nextstrain-data/files/ncov/open"
    else:
        ValueError(f"get_S3_SRC: database {w.database} is unknown.")


## target rule all
rule all_then_clean:
    input:
        "notify_and_upload.gisaid.mock_output.txt",
        "notify_and_upload.genbank.mock_output.txt"
    shell:
        "./bin/clean"


## target rule gisaid
rule gisaid_then_clean:
    input:
        "notify_and_upload.gisaid.mock_output.txt",
    shell:
        "./bin/clean"

## target rule genbank
rule genbank_then_clean:
    input:
        "notify_and_upload.genbank.mock_output.txt",
    shell:
        "./bin/clean"


rule ingest_gisaid:
    input :
        sequences = "data/gisaid/sequences.fasta",
        metadata = "data/gisaid/metadata.tsv",
        nextclade = "data/gisaid/nextclade.tsv",
        additional_info = "data/gisaid/additional_info.tsv",
        flagged_metadata = "data/gisaid/flagged_metadata.txt",
        flagged_annotation = "data/gisaid/transform-log.txt",
        location_hierarchy = "data/gisaid/location_hierarchy.tsv"

rule ingest_genbank:
    input :
        sequences = "data/genbank/sequences.fasta",
        metadata = "data/genbank/metadata.tsv",
        nextclade = "data/genbank/nextclade.tsv",
        additional_info = "data/genbank/additional_info.tsv",
        flagged_metadata = "data/genbank/flagged_metadata.txt",
        flagged_annotation = "data/genbank/transform-log.txt",
        location_hierarchy = "data/genbank/location_hierarchy.tsv"



rule fetch:
    output:
        "data/{database}/data.ndjson"
    params:
        database = "{database}",
        s3_dst = _get_S3_DST
    run:

        if config['fetch'].lower() in ['1','yes','true']:
            shell( './bin/fetch-from-{params.database} > {output}')
        else :
            shell('aws s3 cp --no-progress "{params.s3_dst}/{params.database}.ndjson.gz" - | gunzip -cfq > {output}')


rule transform_gisaid:
    input:
        "data/gisaid/data.ndjson"
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
        "data/genbank/data.ndjson"
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
            --output-fasta {output.fasta} \
            --problem-data {output.problem} > {output.flagged_annotation}

        touch {output.additional_info}
        '''




rule download_old_clades :
    output:
        "data/{database}/nextclade.old.tsv"
    params:
        dst_source=lambda w: _get_S3_DST(w) + '/nextclade.tsv.gz',
        src_source=lambda w: _get_S3_SRC(w) + '/nextclade.tsv.gz',
    shell:
        '''
        set +e
        ( aws s3 cp --no-progress "{params.dst_source}" - || aws s3 cp --no-progress "{params.src_source}" -) | gunzip -cfq > {output}
        #( aws s3 cp --no-progress "{params.src_source}" -) | gunzip -cfq > {output}
        if [ ! -f {output} ]
        then
         exit 1
        fi

        '''


rule filter_fasta :
    input:
        fasta = "data/{database}/sequences.fasta",
        tsv = rules.download_old_clades.output
    output:
        "data/{database}/nextclade.sequences.fasta"
    shell:
        """./bin/filter-fasta --input_fasta={input.fasta} --input_tsv={input.tsv} --output_fasta={output}

        """

rule get_nextclade_inputs:
    output:
        ref = "data/{database}/nextclade-inputs/reference.fasta",
        genemap = "data/{database}/nextclade-inputs/genemap.gff",
        tree = "data/{database}/nextclade-inputs/tree.json",
        qc = "data/{database}/nextclade-inputs/qc.json",
        primers = "data/{database}/nextclade-inputs/primers.csv"
    params:
        url = "https://raw.githubusercontent.com/nextstrain/nextclade/master/data/sars-cov-2"
    shell:
        """
        curl -fsSLJ {params.url}/reference.fasta -o {output.ref}
        curl -fsSLJ {params.url}/genemap.gff -o {output.genemap}
        curl -fsSLJ {params.url}/tree.json -o {output.tree}
        curl -fsSLJ {params.url}/qc.json -o {output.qc}
        curl -fsSLJ {params.url}/primers.csv -o {output.primers}
        """

rule run_nextclade :
    input:
        fasta = rules.filter_fasta.output,
        nextclade_inputs = rules.get_nextclade_inputs.output
    output:
        "data/{database}/nextclade.new.tsv"
    params:
        old_tsv = rules.download_old_clades.output,
        input_folder="data/{database}/nextclade-inputs",
        output_folder="data/{database}/nextclade"
    shell:
        """
        # Check if the file with these extracted sequences is not empty
        if [ ! -s "{input.fasta}" ]; then
           echo "[ INFO] : No new sequences for Nextclade to process. Skipping."
        else

           ./bin/run-nextclade {input.fasta} \
                               {output} \
                               {params.input_folder} \
                               {params.output_folder}
        fi
        if [ ! -f {output} ]
        then
         echo "creating an empty output file"
         head -n 1 {params.old_tsv} > {output}
        fi
        """

rule join_clades :
    input:
        old=rules.download_old_clades.output ,
        new=rules.run_nextclade.output
    output:
        "data/{database}/nextclade.tsv"
    shell:
        "./bin/join-rows {input.new} {input.old} -o {output}"


rule join_metadata_and_clades :
    input:
        clades = rules.join_clades.output ,
        meta = "data/{database}/metadata.noClade.tsv"
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

rule upload_file:
    input:
        "data/{database}/{file}"
    output:
        "logs/{database}_{file}.upload.log"
    params:
        s3_dst=_get_S3_DST,
        compression='gz'
    shell:
        """
        ./bin/upload-to-s3 {input} {params.s3_dst}/{wildcards.file}.{params.compression} 2>&1 | tee {output}
        """

rule upload_ndjson:
    input :
        json = "data/{database}/data.ndjson"
    params:
        s3_dst=_get_S3_DST,
        destination_json = lambda w: _get_S3_DST(w) + "/{database}.ndjson.gz",
        database = "{database}",
        slack_channel = _get_slack_channel
    output:
        msg = "logs/{database}_data.ndjson.msg"
    shell:
        '''
        dst={params.destination_json}

        src_record_count="$(wc -l < "{input.json}")"
        dst_record_count="$(wc -l < <(aws s3 cp --no-progress "$dst" - | gunzip -cfq))"
        added_records="$(( src_record_count - dst_record_count ))"

        msg=""

        if [[ $added_records -gt 0 ]]; then
            msg="📈 New nCoV records (n=$added_records) found on {params.database}."
        elif [[ $added_records -lt 0 ]]; then
            msg="WARNING: the new version of {params.database} has fewer records‽"
        else
            msg="📈 No new nCoV records found on {params.database}."
        fi

        ./bin/notify-slack "$msg" $SLACK_TOKEN {params.slack_channel}
        echo "$msg" > {output.msg}

        ./bin/upload-to-s3 {input.json} {params.destination_json} 2>&1 >> {output}
        '''

rule upload_and_notify_generic:
    input :
        upload_file = "data/{database}/{file}",
        log = "logs/{database}_{file}.upload.log"
    params:
        slack_channel = _get_slack_channel
    output:
        msg = "logs/{database}_{file}.log"
    shell:
        '''
        ./bin/notify-slack "Updated {input.upload_file} available."  $SLACK_TOKEN {params.slack_channel} 2>&1 |tee {output.msg}
        '''

rule metadata_change:
    input:
        metadata = "data/{database}/metadata.tsv",
    output:
        "logs/{database}_metadata_change.msg"
    params:
        destination_metadata = lambda w: _get_S3_DST(w)+f"/{w.database}_metadata.tsv.gz",
        idcolumn=lambda w : config['idcolumn'][w.database],
        slack_channel = _get_slack_channel
    shell:
        """
            # notify and upload metadata change

            dst_local="$(mktemp -t metadata-XXXXXX.tsv)"
            diff="$(mktemp -t metadata-changes-XXXXXX)"
            additions="$(mktemp -t metadata-additions-XXXXXX)"
            trap "rm -f '$dst_local' '$diff' '$additions'" EXIT

            ./bin/compute-metadata-change {input.metadata} "{params.destination_metadata}" {params.idcolumn} $dst_local $diff $additions


            # csv-diff outputs two newlines which -n ignores but -s does not
            if [[ -n "$(< "$diff")" ]]; then
                # "Notifying Slack about metadata change."
                ./bin/notify-slack --upload "metadata-changes.txt" $SLACK_TOKEN {params.slack_channel} < "$diff"
            else
                echo "No metadata change."
            fi | cat > {output}
            # checking additions
            if [[ -s "$additions" ]]; then
                # "Notifying Slack about metadata additions."
                ./bin/notify-slack --upload "metadata-additions.tsv" $SLACK_TOKEN {params.slack_channel} < "$additions"

                if [[ "{params.idcolumn}" == "gisaid_epi_isl" ]]; then
                    ./bin/notify-users-on-new-locations "$additions" --slack-token $SLACK_TOKEN --slack-channel {params.slack_channel}
                fi
            fi | cat >> {output}
        """

rule location_hierarchy_additions:
    input:
        location_hierarchy = "data/{database}/location_hierarchy.tsv"
    output:
        "logs/{database}_location_hierarchy_changes.msg"
    params:
        slack_channel = _get_slack_channel
    shell:
        """
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

                ./bin/notify-slack "$message" $SLACK_TOKEN {params.slack_channel} > {output}
                ./bin/notify-slack --upload "location-hierarchy-additions.tsv" $SLACK_TOKEN {params.slack_channel} < "$diff" >> {output}
            else
                echo "No location hierarchy changes" > {output}
            fi
        """

rule additional_info_changes:
    input:
        additional_info = "data/{database}/additional_info.tsv"
    output:
        "logs/{database}/additional_info_changes.msg"
    params:
        slack_channel = _get_slack_channel,
        destination_additional_info = lambda w: _get_S3_DST(w)+"/{w.database}_additional_info.tsv.gz",
    shell:
        """
            diff="$(mktemp -t additionnal-info-changes-XXXXXX)"
            trap "rm -f '$diff'" EXIT

            ./bin/compute-additional-info-change {input.additional_info} "{params.destination_additional_info}" $diff

            if [[ -n "$diff" ]]; then
                # "Notifying Slack about additional info change."
                ./bin/notify-slack --upload "additional-info-changes.txt" $SLACK_TOKEN {params.slack_channel} < "$diff"
            else
                echo "No additional info change."
            fi | cat > {output}
        """


rule new_flagged_metadata:
    input:
        flagged_metadata = "data/{database}/flagged_metadata.txt",
    output:
        "logs/{database}/new_flagged_metadata.msg"
    params:
        slack_channel = _get_slack_channel,
        destination_flagged_metadata = lambda w: _get_S3_DST(w)+"/{w.database}_flagged_metadata.tsv.gz",
    shell:
        """
            dst_local="$(mktemp -t flagged-metadata-XXXXXX.txt)"
            diff="$(mktemp -t flagged-metadata-additions-XXXXXX)"
            trap "rm -f '$dst_local' '$diff'" EXIT

            ./bin/compute-flagged-metadata-change {input.flagged_metadata} "{params.destination_flagged_metadata}" $dst_local $diff
            if [[ -s "$diff" ]]; then
                # "Notifying Slack about flagged metadata additions."
                ./bin/notify-slack ":waving_black_flag: Newly flagged metadata" $SLACK_TOKEN {params.slack_channel}
                ./bin/notify-slack --upload "flagged-metadata-additions.txt" $SLACK_TOKEN {params.slack_channel} < "$diff"
            else
                echo "No flagged metadata additions."
            fi | cat > {output}
        """


rule notify_and_upload:
    input :
        json = "logs/{database}_data.ndjson.msg",
        sequences = "logs/{database}_sequences.fasta.log",
        metadata = "logs/{database}_metadata.tsv.log",
        nextclade = "logs/{database}_nextclade.tsv.log",
        additional_info = "logs/{database}_additional_info.tsv.log",
        flagged_metadata = "logs/{database}_flagged_metadata.txt.log",
        location_hierarchy = "logs/{database}_location_hierarchy.tsv.log",
        location_hierarchy_msg = "logs/{database}_location_hierarchy_changes.msg",
        flagged_annotation = "data/{database}/transform-log.txt",
        metadata_changes = rules.metadata_change.output,
        new_flagged_metadata = rules.new_flagged_metadata.output,
        additional_info_changes = rules.additional_info_changes.output
    output :
        "notify_and_upload.{database}.mock_output.txt"
    shell:
        """
        touch {output}
        """