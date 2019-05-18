FROM hysds/pge-base:latest
MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="scrapes the Earthdata CMR for metadata"

USER ops

COPY . /home/ops/verdi/ops/cmr_scraper

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
