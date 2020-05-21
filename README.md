# The Benvolio I/O service

Benvolio's goal is to provide many of the tried and true ROMIO optimizations in
an I/O service framework.  Based on [mochi](https://press3.mcs.anl.gov/mochi/)
services, benvolio will provide efficient i/o transformations to both MPI and
non-MPI applications.

[ROMIO](https://press3.mcs.anl.gov/romio) is a great way to optimize synchronous
parallel I/O in MPI codes, like writing a checkpoint or reading initial
conditions.  What if you are not using MPI in your application?  What if your
I/O is not synchronous?  Consider benvolio.

## Getting Started

You're probably reading this on gitlab or from a checked out benvolio
repository, but just in case you are not, you can get the latest benvolio from
<https://xgitlab.cels.anl.gov/sds/benvolio>

### prerequisites

* GNU Autotools to build
* [thallium](https://xgitlab.cels.anl.gov/sds/thallium) C++ wrappers to Mercury, Margo, and Argobots
* [ssg](https://xgitlab.cels.anl.gov/sds/ssg) group management for server identification
* [abt-io](https://xgitlab.cels.anl.gov/sds/abt-io/) providing I/O bindings for Argobots

We find the [spack](https://spack.readthedocs.io/en/latest/) package manager
very helpful for building everything you need.  We have some additional
packages in the [sds-repo](https://xgitlab.cels.anl.gov/sds/sds-repo).  Once
you have both spack and our extra repo installed you can build all the benvolio
prerequisites with

    spack install mochi-thallium mochi-abt-io mochi-ssg+pmix
    spack load -r mochi-thallium mochi-abt-io mochi-ssg+pmix


### building

With all the dependencies in place, the rest is straightforward:

* generate the configure script with `autoreconf -fi`
* `configure  --prefix=...`
* `make all tests`


### Simple execution

There are two parts to benvolio: the _provider_ (sometimes called the _server_)
and the _client_ .  A more complicated example would run the provider on one or
more nodes and the client on one or more (possibly the same) node.  In this
case we are just going to run everything on one node.

1. PMIx:  We recently switched benvolio to use [PMIx](https://pmix.org/).  Some
job schedulers like SLURM are PMIx-aware, but more likely you will have to use
the PMIx reference runtime (prrte).  Spack can install that for you (`spack
install prrte` but NOTE: you'll need the one line fix in
<https://github.com/spack/spack/pull/16530/files> if you are using spack-0.14.2
or older).  Start the PRRTE managers:

    $ prte --host localhost:8 &

2. benvolio providers:  use `prun` to launch multiple providers.  A benvolio
provider takes quite a few command line arguments.  Only the `-f` and `-p`
arguments are mandatory.
  * `-f statefile` : file name to save provider state.  Clients will load this to find the providers.
  * `-p protocol` : what kind of protocol to use.  A mercury protocol identifier.
  * `-b buffersize`: size of internal memory buffer
  * `-x xfersize`  : the internal memory buffer will be partitioned into 'xfersize' pieces for pipelined transfers
  * `-t threads`   : how many I/O threads to spawn.
  * `-s streams`    : how many margo streams (Argobot Execution Streams) to use


    $ prun -np 2 ./src/provider/bv-server -f bv.svc -p sm:  &

3. A simple benvolio test:  `tests/simple` runs through a few I/O patterns.  It
has one mandatory argument: the name of the provider statefile.  You can give
it an optional file name, or it will write to and read from a file called
'dummy' by default. If you see `Error: PMIx event notification registration
failed! [-31]` you can ignore that for now.  We're still investigating
( <https://xgitlab.cels.anl.gov/sds/benvolio/issues/2> )

   $ ./tests/simple bv.svc


4. cleanup.  the `bv_shutdown` tool is a simple client with only one job: ask
the provider to shut itself down.  Then we stop the prrte daemons with the
'-terminate' command

    $ ./src/client/bv-shutdown bv.svc
    $ prun -terminate
