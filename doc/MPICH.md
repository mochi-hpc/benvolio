# Benvolio and MPICH

- check out https://github.com/roblatham00/mpich/tree/benvolio

- set CPPFLAGS and LDFLAGS to point to benvolio installation

- add 'bv' to the `--with-file-system` list: e.g. --with-file-system=lustre+bv

- Start however many Benvolio targets you want.

- set the environemnt variable BV_STATEFILE to point to the server statefile.

- if you want extra information about benvolio behavior, set the BV_SHOW_STATS
