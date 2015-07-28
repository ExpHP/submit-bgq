
import sys
from setuptools import setup

dependencies = []
if (sys.version_info.major, sys.version_info.minor) < (2,7):
	dependencies.append('argparse')

setup(
	name='Submit (BGQ ver)',
	version = '0.0',
	description = 'personal job submission script for Blue Gene Q',
	url = 'https://github.com/ExpHP/submit-bgq',
	author = 'Michael Lamparski',
	author_email = 'lampam@rpi.edu',

	scripts=['submitq.py'],
	install_requires=dependencies,

#	packages=find_packages(), # include sub-packages
)
