# MOCHIO and MPICH

- check out https://github.com/roblatham00/mpich/tree/mochio

- set CPPFLAGS and LDFLAGS to point to mochio installation

- add 'mochio' to the `--with-file-system` list: e.g. --with-file-system=lustre+mochio

- Start however many MOCHIO targets you want.

- set the environemnt variable MOCHIO_STATEFILE to point to the server statefile.

- if you want extra information about mochio behavior, set the MOCHIO_SHOW_STATS
