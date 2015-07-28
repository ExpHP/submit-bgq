# submit-bgq
This is a script for my own personal use.  It submits a large number of SLURM jobs,
and is meant for use on Blue Gene Q which has an extremely large number of nodes.
It's on github because this makes it easier to push updates back and forth between my own
computer and the BGQ.

It is Python 2.6 compatible.

# installation

    python setup.py build
    sudo python setup.py install
