# Benvolio and MPICH

Let's say you want to build the IOR benchmark to try out the benvolio ROMIO
driver.  Unfortunately that will probably require building MPICH twice.
1. benvolio requires an MPI implementation so that it can bootstrap SSG (might
   be a good reason to look at PMIx)
1. The benvolio clients communicate with Mercury, not MPI, so it's
   theoretically possible to build just the server with an MPI implementation
   and the build the clients as part of another.  I'll have to update the build
   system to pull that off.

To avoid invalid communicators (when a program linked with one MPI
implementation tries to use another one), I build benvolio with MPICH and then
build MPICH with ROMIO with benvolio support.  

- check out https://github.com/roblatham00/mpich/tree/benvolio

- ensure `pkg-config` knows about the `bv-client` package.  ROMIO's configure will pick up the necessary CFLAGS and LDFLAGS

- add 'bv' to the `--with-file-system` list: e.g. --with-file-system=lustre+bv

- Start however many Benvolio targets you want.

- set the environment variable BV_STATEFILE to point to the server statefile.

- if you want extra information about benvolio behavior, set the BV_SHOW_STATS
