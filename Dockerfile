# This image, nextstrain/ncov-ingest, is used to run the ncov-ingest pipelines
# on AWS Batch via the `nextstrain build --aws-batch` infrastructure.
#
# The additional dependencies of ncov-ingest are layered atop a version of the
# nextstrain/base image.  In the future, `nextstrain build` could (and should)
# automatically satisfy the additional dependencies of a build (e.g. with
# integrated Buildpacks and/or Conda support; there are pros and cons for each
# system).
#
# The image is updated and published automatically by a GitHub Actions
# workflow.  To rebuild the image manually on your local computer, run:
#
#     docker build -t nextstrain/ncov-ingest:latest .
#
# Publish to Docker Hub with:
#
#     docker image push nextstrain/ncov-ingest:latest
#
# The automatic ingest runs always use the "latest" tag.
#
# Note that this image is only intended to provide the *dependencies* of
# ncov-ingest's pipelines, not the actual programs and pipelines of
# ncov-ingest themselves.  This means the image only needs to be updated when
# dependencies change, not when any pipeline change is made, and thus image
# updates can be far less frequent.

# XXX TODO: This can be updated to :latest eventually when the python-base
# version becomes the new default, if this ncov-ingest image itself is still
# relevant at that time.
#   -trs, 19 Jan 2020

# Let's drop dependency on an image of unknown origin and start over.
#   -ivan.aksamentov 2021-12-13
FROM python:3.7-buster

SHELL ["/bin/bash", "-c"]

RUN set -euxo pipefail \
&& apt-get update \
&& apt-get install -y --no-install-recommends \
  build-essential \
  time \
  xz-utils

COPY Pipfile Pipfile.lock /workdir/

# Install Python deps
RUN set -euxo pipefail \
&& python3 -m pip install pipenv \
&& PIPENV_PIPFILE=/workdir/Pipfile pipenv sync --system

# Put any bin/ dir in the cwd on the path for more convenient invocation of
# ncov-ingest's programs.
ENV PATH="./bin:$PATH"

# Add some basic metadata to our image for searching later.  Note that some
# common labels are inherited from our base image.
ARG GIT_REVISION
LABEL org.opencontainers.image.source="https://github.com/nextstrain/ncov-ingest"
LABEL org.opencontainers.image.revision="${GIT_REVISION}"
LABEL org.nextstrain.image.name="nextstrain/ncov-ingest"
