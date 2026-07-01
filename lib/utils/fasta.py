"""
Fast streaming FASTA I/O.

Biopython's ``SeqIO.parse``/``SeqIO.write`` build a ``SeqRecord`` object per
record and reformat output, which is ~10-50x slower than line streaming for the
multi-million-record FASTA files this pipeline handles. ``stream_fasta`` yields
``(id, sequence)`` tuples directly.

The record ``id`` is the first whitespace-delimited token of the header, matching
``Bio.SeqRecord.SeqRecord.id`` so that membership checks against ids stay identical
to the previous Biopython-based code. Multi-line sequence records are supported;
the yielded sequence contains no newlines.
"""
from typing import Iterator, TextIO, Tuple


def stream_fasta(fh: TextIO) -> Iterator[Tuple[str, str]]:
    """Yield ``(id, sequence)`` for each record in an open FASTA file handle."""
    seq_id = None
    chunks = []
    for line in fh:
        if line.startswith(">"):
            if seq_id is not None:
                yield seq_id, "".join(chunks)
            header = line[1:].split(None, 1)
            seq_id = header[0] if header else ""
            chunks = []
        else:
            chunks.append(line.strip())
    if seq_id is not None:
        yield seq_id, "".join(chunks)
