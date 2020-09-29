# Running on Cray

## building

Intel compiler might not support all the C++14 or argobots atomics features of
our stack, so use the gnu compiler with

    module swap PrgEnv-intel PrgEnv-gnu

Some of our packages have both static and shared libs, others have just shared.
To get Cray toolchain to only build and link with shared libs, I had to
configure like this:

    configure CFLAGS=-dynamic CC=cc CXX=CC --host x86-linux-gnu

Otherwise, configure will fail to find the Mercury libraries and the configure
test for ssg features will fail.

Argonne's Theta sysem is a Cray XC40.  It uses 'aprun' to launch jobs.

As long as you have requested enough nodes, you can launch as many 'aprun'
processes as you want.  However, as a security precaution those processes
cannot talk to each other without first setting up a "protection domain"

## managing protection domains

Other projects have done a good job documenting protection domains.  In
particular I found
https://github.com/ovis-hpc/ovis/wiki/Protection-Domain-Tags-(Cray-Specific)
helpful.  Here are some areas where Theta differs from other sites.

One manages protection domains with utiliites like `apstat` and `apmgr`.  On
theta those utilities are not available on the login nodes.  They are only
available on the monitor nodes (the nodes from which your job scripts are run,
or where your interactive shell lands).

Protection domains persist until someone explicitly releases them.  While
convienent in many cases, and necessary if one submission script starts a
server and another starts a client, it does mean that the protection domains
can end up hanging around longer than expected.  You might get an error like
this:

    apmgr pdomain create bv-test failed: cannot allocate protection domain; reached limit of 10

No way around that except to contact the support desk and ask for help cleaning up old ones.

## Building MPICH

Like any other external library, the 'bv' driver (
https://github.com/roblatham00/mpich/tree/bv
) requires CPPFLAGS, LDFLAGS, and LIBS to be set.  Furthermore, builidng on
the Cray requires a few extra steps, documented here:
https://wiki.mpich.org/mpich/index.php/Cray


## PMI on Cray

Benvolio has support for SSG's PMI support ("now you have two problems").  PMI
support on Theta seems a bit of a challenge.  We reccomend the MPI bootstrap on
Theta.
