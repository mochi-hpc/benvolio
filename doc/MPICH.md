# Benvolio and MPICH

You have two choices for how to launch benvolio providers.  If you use MPI to
launch the providers, we will have to build MPICH twice: MPICH requires
benvolio headers to build the benvolio driver for ROMIO, but the benvolio
provider is also going to look for an MPI implementation.

We only have a ROMIO driver for MPICH, so you'll have to build your client
codes with a personal build of MPICH (not vendor-supplied).
If you would like to build the benvolio server with "vendor mpi", I think that
is possible, but it's a bit more work than the "build mpich twice" way, if you
can believe it.

Future work is to separate the buidling of the provider from the building of
the client libraries.

If using PMIx to launch the providers, none of this is necessary (though PMIx
has given us different problems on some platforms).

## Building

Option one: building from git:

- If you are not familiar with building MPICH, please see
  https://wiki.mpich.org/mpich/index.php/Getting_And_Building_MPICH for how to build it.

- check out https://github.com/roblatham00/mpich/tree/benvolio or add it as a remote repository.

- The benvolio work is in the 'benvolio' branch of the above fork.  For example, assuming you have named your remote repository `github-robl`

    git checkout -b benvolio github-robl/benvolio

Option two: building from mpich-3.4b1 snapshot:

- An easier way might be to build the "benvolio" snapshot:  I've uploaded it to Box: https://anl.box.com/shared/static/6epyqzrd11xbc8k2mbedbzdsggbvf9u1

    curl -L 'https://anl.box.com/shared/static/w4f1msfbfjeqbj15jyikmzwgn2bg5ovr' -o mpich-3.4b1-benvolio.tar.gz

- Stage one: build mpich.  Add this custom-built mpich to  your spack `packages.py` as an external package

- Stage two: build benvolio.  Spack can even do this for you

- Stage three: build MPICH again.  Before you build MPICH this time, ensure `pkg-config`
  knows about the `bv-client`
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
<https://github.com/mochi-hpc/benvolio/blob/main/tests/wrap_runs.sh>)

1. Start a PMIx runtime and the benvolio provider processes (see
<https://github.com/mochi-hpc/benvolio/blob/main/README.md>)

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

Benvolio itself is built the same way on just about any platform, thanks to
libfabric hiding the details for us.  MPICH and job launchers, however, mean
configuration is a little different on every machine

### NERSC Cori (Cray, Slurm)

- Spack-0.16.0 seems to have changed the behavior of library resolution on
  Cori, resulting in programs unable to find libraries. Until we figure out
  this problem, stick with spack-0.15.3 and the latest `sds-repo`
- load the GNU compilers with `module swap PrgEnv-intel PrgEnv-gnu`
- Cori interconnect is gni: use `--with-device=ch4:ofi`

    module swap PrgEnv-intel PrgEnv-gnu
    tar xf mpich-3.4.1-benvolio.tar.gz
    cd mpich-3.4.1-benvolio
    mkdir build
    cd build
    ../configure  --prefix=${HOME}/soft/mpich-3.4.1-benvolio --with-device=ch4:ofi --with-pm=none --with-pmi=cray && make -j 8 && make install

- Add this home-built version of mpich as an `external package` to spack's packages.yaml.  For example, I have this in my packages.yaml file using spack-0.16.0 syntax


```
  mpich:
    externals:
    - spec: "mpich@7.3.1%gcc@10.1.0"
      modules:
      - cray-mpich
    - spec: "mpich@7.3.1%intel@16.0.0.109"
      modules:
      - cray-mpich
    - spec: "mpich@3.4.1%gcc@8.3.0"
      prefix: /global/homes/r/robl/soft/mpich-3.4.1-benvolio/
```

The older 0.15.3 syntax would look like this:

```
    mpich:
        paths:
            mpich@3.4.1: /global/homes/r/robl/soft/mpich-3.4.1-benvolio
        buildable: False
        modules:
            mpich@7.7.6: cray-mpich/7.7.6
        buildable: False
```

While we are editing packages.yaml, add an entry for the 'rdma-credentials' facility:

```
   rdma-credentials:
        modules:
           rdma-credentials@1.2.25: rdma-credentials/1.2.25-7.0.1.1_5.14__g86c1dd8.ari
```


- build benvolio using this mpich and load it into your environment
- take note of the compiler: use whichever gcc compiler was loaded in your
  environment when you built MPICH.  Otherwise, you will get some confusion
  when g++10 tries to use symbols that are not available in g++8's c++ runtime.
- note too we asked for the `cray-drc` variant, a feature on Cray systems that
  allows separate processes to exchange a token and thereby obtain permission
  to RDMA into each others addresses.

    spack install benvolio%gcc@8.3.0  +cray-drc +mpi +pmix '^mpich@3.4.1' '^pmix@master
    spack load -r benvolio

- now build mpich again requesting the benvolio file system

    # double-check benvolio is loaded in your environment
    $ pkg-config bv-client --cflags
    # temporarily take `mpich` out of your environment
    $ spack unload mpich
    $ ../configure --prefix=${HOME}/soft/mpich-3.4.1-benvolio \
        --with-device=ch4:ofi \
        --with-pmi=cray \
	--with-file-system=lustre+benvolio && make -j 8 && make install

### OLCF Summit (IBM POWER9, mpiexec)

- unload the darshan module:  it was built with IBM Spectrum-MPI (OpenMPI
  based) and you will get unusual errors about invalid communicator.  Once you
  have told spack about our custom-buit MPICH, build darshan from spack.
- load the `gcc/9.1.0` module
- On summit we use UCX: build mpich with `--with-device=ch4:ucx CFLAGS=-std=gnu11`
- double-check that `mpicc` is not in your path

    module unload darshan
    module load gcc/9.1.0
    spack unload mpich
    tar xf mpich-3.4.1-benvolio.tar.gz
    cd mpich-3.4.1-benvolio
    mkdir build
    cd build
    ../configure  --prefix=${HOME}/soft/mpich-3.4.1-benvolio \
            --with-device=ch4:ucx CFLAGS=-std=gnu11 \
            && make -j 8 && make install

- Add this custom-built mpich as an `external package` to spack's package.yaml.  For example I have this in my spack-0.15.3 packages.yaml:


```
packages:
    all:
        compiler: [gcc@9.1.0, xl]
        providers:
            mpi: [spectrum-mpi, mpich]
            pkgconfig: [pkg-config]
    spectrum-mpi:
        modules:
            spectrum-mpi@10.3.1.2%gcc: spectrum-mpi/10.3.1.2-20200121
        buildable: False
    mpich:
        paths:
            mpich@3.4.1: /ccs/home/robl/soft/mpich-3.4.1-benvolio
    ...
```

- As in the Cori example above, build benvolio with this mpich and load it into your environment.  As mentioned in README.summit we need to select some specific versions and variants to work around issues:

    spack install benvolio +mpi+pmix '^mpich' '^pmix@master' '^argobots@1.1b1' '^mercury ~boostsys ~checksum'
    spack load -r benovlio

- now build mpich again requesting the benvolio file system

    # double-check benvolio is loaded in your environment
    $ pkg-config bv-client --cflags
    # temporarily take `mpich` out of your environment
    $ spack unload mpich
    $ ../configure --prefix=${HOME}/soft/mpich-3.4.1-benvolio \
        --with-device=ch4:ucx CFLAGS=-std=gnu11 \
	--with-file-system=lustre+benvolio && make -j 8 && make install
    $ spack load -r mpich

- mpich interaction with the 'jsrun' job launcher is hit or miss.  Instead,
  generate a list of nodes and pass that list to mpiexec.  Additionally, the
  '--launch-method=ssh' flag will ensure MPICH uses the slower but more
  reliable ssh-based method

```
SERVERS=2
CLIENTS=6

jsrun -r 1 hostname > hostfile
head -${SERVERS} hostfile > servers
tail -n +$(($SERVERS+1)) hostfile > clients

# launch the benvolio providers
mpiexec -f servers -launcher ssh -ppn 1 -n ${SERVERS} ... &
# launch the mpi job
mpiexec -f clients -launcher ssh -ppn $((22)) -n $((CLIENTS*22))


### ALCF Theta (Cray, aprun)

