# Benvolio and MPICH

We used to require building MPICH twice, but that is no longer necessary now
that benvolio uses PMIx to launch the providers.

## Building
- If you are not familiar with building MPICH, please see
  https://wiki.mpich.org/mpich/index.php/Getting_And_Building_MPICH for how to build it.

- check out https://github.com/roblatham00/mpich/tree/benvolio or add it as a remote repository.

- The benvolio work is in the 'benvolio' branch of the above fork.  For example, assuming you have named your remote repository `github-robl`

    git checkout -b benvolio github-robl/benvolio

- Before you build MPICH, ensure `pkg-config` knows about the `bv-client`
  package.  ROMIO's configure will pick up the necessary CFLAGS and LDFLAGS,
  but if `pkg-config --libs bv-client` returns an error, ROMIO configure will
  just plow along without benvolio support. Double check that not only
  `bv-client` but also the libraries benvolio depends on are available too
  (thallium, abt-io, ssg).  If you are using spack to manage dependencies, you
  can do `spack load -r mochi-ssg mochi-abt-io mochi-thallium`

- add 'benvolio' to the `--with-file-system` list: e.g. --with-file-system=lustre+benvolio

## Running benvolio on workstations or laptop

To test benvolio on a laptop, one can follow the same appraoch we take in the
benvolio tests (see
<https://xgitlab.cels.anl.gov/sds/benvolio/blob/master/tests/wrap_runs.sh>)

1. Start a PMIx runtime and the benvolio provider processes (see
<https://xgitlab.cels.anl.gov/sds/benvolio/blob/master/README.md>)

2.  We tell MPICH codes about the benvolio state file with an environment
variable: set the environment variable `BV_STATEFILE` to point to the server
statefile.

3. if you want extra information about benvolio behavior, set the `BV_SHOW_STATS` environment variable.

- benvolio is not a true file system, so ROMIO automagic file system detection
  will not work.  Instead, you will have to prefix file names with 'benvolio:'.

    $ mpiexec -np 8 ior -H -a MPIIO -c -Z -b 50k -t 50k -o benvolio:testfile

3. cleanup:  If you shut down benvolio gracefully, tools like Darshan, google
  perf tools, and others that rely on writing output at exit will be able to
  provide a report.  run `bv_shutdown` and `prun -terminate` as described in
  README.md

## benvolio on facilities

TODO
