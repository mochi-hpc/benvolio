# Benvolio and MPICH

We used to require building MPICH twice, but that is no longer necessary now
that benvolio uses PMIx to launch the providers.

- If you are not familiar with building MPICH, please see
  https://wiki.mpich.org/mpich/index.php/Getting_And_Building_MPICH for how to build it.

- check out https://github.com/roblatham00/mpich/tree/benvolio

- Before you build MPICH, ensure `pkg-config` knows about the `bv-client`
  package.  ROMIO's configure will pick up the necessary CFLAGS and LDFLAGS,
  but if `pkg-config --libs bv-client` returns an error, ROMIO configure will
  just plow along without benvolio support. Double check that not only
  `bv-client` but also the libraries benvolio depends on are available too
  (thallium, abt-io, ssg).  If you are using spack to manage dependencies, you
  can do `spack load -r mochi-ssg mochi-abt-io mochi-thallium`

- add 'bv' to the `--with-file-system` list: e.g. --with-file-system=lustre+bv

- Start however many Benvolio targets you want.

- set the environment variable BV_STATEFILE to point to the server statefile.

- if you want extra information about benvolio behavior, set the BV_SHOW_STATS

- benvolio is not a true file system, so ROMIO automagic file system detection
  will not work.  Instead, you will have to prefix file names with 'benvolio:'.
