# Running on Cray

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

Protection domains persist until someone explicitly releases them.  While convienent in many cases, and necessary if one submission script starts a server and another starts a client, it does mean that the protection domains can end up hanging around longer than expected.  You might get an error like this:

    apmgr pdomain create mochio-test failed: cannot allocate protection domain; reached limit of 10

No way around that except to contact the support desk and ask for help cleaning up old ones.
