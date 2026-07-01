"""
Disk-backed replacement for sorting a whole transform pipeline in memory.

The GenBank/GISAID/RKI transforms deduplicate records by strain, keeping the
best (longest, then earliest) record per strain.  They used to do this by
pulling every record into a list and calling ``sorted()`` -- which, at
production scale (~9M records), was the dominant driver of each rule's peak
memory.  These helpers stream the records through an external ``sort`` that
spills to disk instead, so peak memory stays flat regardless of corpus size.
"""
import json
import os
import subprocess
import tempfile

from . import LINE_NUMBER_KEY


def spill_to_sorted_tempfile(records, id_key, output_dir):
    """Stream ``records`` to a temp file and sort it on disk.

    Sorts by ``(strain asc, length desc, id_key asc, line-number asc)`` -- the
    exact ordering the transforms previously produced with an in-memory
    ``sorted()``.  Returns the path to the sorted temp file; the caller reads it
    back with :func:`read_sorted_records` and is responsible for unlinking it.

    Each record becomes a line of five tab-separated fields: the four sort keys
    followed by the record as a JSON blob.  ``json.dumps`` escapes any tabs or
    newlines inside the record, so the blob is always a single safe field.
    """
    sort_tmp = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="\n", suffix=".presort.tsv",
        dir=output_dir or ".", delete=False,
    )
    with sort_tmp:
        for record in records:
            sort_tmp.write(
                f"{record['strain']}\t{record['length']}\t"
                f"{record[id_key]}\t{record[LINE_NUMBER_KEY]}\t"
                f"{json.dumps(record, default=str)}\n"
            )

    # LC_ALL=C makes sort compare bytewise, which matches Python's code-point
    # ordering over the UTF-8 strain/id fields.  The (strain, length, id,
    # line-number) tuple is a total order (line number is unique per record), so
    # sort stability is irrelevant.
    subprocess.run(
        ["sort", "-t", "\t", "-k1,1", "-k2,2nr", "-k3,3", "-k4,4n",
         "-o", sort_tmp.name, sort_tmp.name],
        check=True,
        env={**os.environ, "LC_ALL": "C"},
    )
    return sort_tmp.name


def read_sorted_records(path):
    """Yield the records written by :func:`spill_to_sorted_tempfile`, in order."""
    with open(path, "r", encoding="utf-8") as sorted_in:
        for line in sorted_in:
            yield json.loads(line.split("\t", 4)[4])
