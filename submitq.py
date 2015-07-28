#!/usr/bin/env python

from __future__ import print_function, with_statement, division

import os, sys
import subprocess, shlex
import logging as logger

# For Python 2.6 which does not have Counter.
# Only the following parts of the Counter API are supported:
#   - x = PseudoCounter()  (nullary constructor)
#   - x['counter']         (__getitem__)
#   - x['counter'] += 1    (__iadd__)
import functools
import collections
PseudoCounter = functools.partial(collections.defaultdict, lambda *args: 0)


# Python 2.6 doesn't have argparse, which is unfortunate for BlueGene.
# Thankfully, it started its life as an external package, and can still be found on PyPI.
def require_argparse():
	global argparse # this does indeed work on import statements
	try: import argparse
	except:
		logger.error('argparse not available.')
		logger.error('Get it here https://pypi.python.org/pypi/argparse')
		logger.error('Put argparse.py somewhere and add that directory to PYTHONPATH.')
		logger.error('(e.g.  export PYTHONPATH=$PYTHONPATH:/home/me/my-python-packages )')
		sys.exit(1)

#--------------------------------------
# Constants and environment vars

SUBMIT_SFLAGS_DEFAULT = '-p small -n 64 -t 02:00:00 -o out-%j'
SUBMIT_VASP_DEFAULT   = 'vasp.slm'

VASP_CMD     = os.getenv('SUBMIT_VASP',   SUBMIT_VASP_DEFAULT)
SBATCH_FLAGS = os.getenv('SUBMIT_SFLAGS', SUBMIT_SFLAGS_DEFAULT)

FINISHED_FNAME = 'finished'
SUBMITTED_FNAME = 'submitted'

#--------------------------------------
# Types

# (as close as we can easily get to a strictly-typed enum on Python 2.6)
class Mode:
	SAFE   = object()
	CHECK  = object()
	SKIP   = object()
	RESUME = object()

#--------------------------------------
# Argument parsing and help

HELP_ENV = '''
environment variables:
 SUBMIT_SFLAGS    sbatch options
                  default: {sflag}
 SUBMIT_VASP      command for vasp, plus options
                  default: {vasp}
'''.format(sflag=SUBMIT_SFLAGS_DEFAULT, vasp=SUBMIT_VASP_DEFAULT)

HELP_EXTRA_FILES = '''
This script enters all the specified directories (containing VASP input files)
and queues up a bunch of jobs to slurm.

Two files are created in each folder as a form of "persistent state":
 * 'submitted' marks a calculation that is started but incomplete.
    It is created when a job is submitted.
    It is deleted when 'finished' is created.
 * 'finished' marks a trial that is 100% complete.
    It is created when the script is run again after a job has finished.
'''

HELP_OPT_RESUME = '''
Resume incomplete trials via new jobs. (DANGEROUS!)
This is currently the only way to have the script resume a trial
that was interrupted. You MUST ensure that NO TRIALS ARE RUNNING
IN THE DIRECTORY before using it!
'''

def main():
	# configure logger for basic console output
	logger.basicConfig(level=logger.INFO, format='%(message)s')

	args = process_args()

	mode = Mode.SAFE
	if args.skip:   mode = Mode.SKIP
	if args.resume: mode = Mode.RESUME
	if args.check:  mode = Mode.CHECK

	stats = process_all_trials(args.input, mode)
	print_summary(stats, mode)

def process_args():
	require_argparse()

	parser = argparse.ArgumentParser(
		description = 'Submit a bunch of jobs for VASP.\n\n{files}\n\n{env}'.format(files=HELP_EXTRA_FILES, env=HELP_ENV),
		formatter_class = argparse.RawDescriptionHelpFormatter,
	)

	# positional args
	parser.add_argument('input', metavar='DIR', type=str, nargs='+', help='Vasp input directories (already prepared with INCAR, KPOINTS, etc.)')

	# options
	group = parser.add_mutually_exclusive_group()
	group.add_argument('-r', '--resume', action='store_true', help=HELP_OPT_RESUME)
	group.add_argument('-s', '--skip', action='store_true', help='Skip incomplete trials.')
	group.add_argument('-c', '--check', action='store_true', help='Only add "finished" markers; do not submit anything.')

	return parser.parse_args()

#--------------------------------------
# High level methods

SAFEMODE_ERROR_MSG = '''
Found some incomplete trials!  Cannot continue in safe mode!!

Are any jobs currently running in this directory?
 If YES:  Use '-s' to SKIP incomplete trials.
 If  NO:  You may use '-r' to RESUME them. (via new jobs)

Caution: using -r when a job is running may result in multiple
  instances of vasp working in the same directory! (prettybad)
'''
def do_safemode_check(dirs):
	unsafe_count=0
	for d in dirs:
		if is_marked_submitted(d) and not is_marked_finished(d):
			unsafe_count += 1
			logger.info('%s: unfinished, but already submitted!', d)

	if unsafe_count > 0:
		logger.error(SAFEMODE_ERROR_MSG)
		sys.exit(1)


# While I don't like writing large, monolithic functions, I've found that
#  any attempt to break this one up so far has only made it more difficult
#  to reason about its correctness.
def process_all_trials(dirs, mode):

	stats = PseudoCounter() # For the end summary
	remaining = set(dirs)   # a set for easy item removal (facilitates multiple passes)

	# These actions are almost aways done together.
	def remove_and_log(d, counter, msg):
		if counter is not None:
			stats[counter] += 1  # increment a summary stat
		remaining.remove(d)  # omit the directory from subsequent passes
		logger.info('%s: %s' % (d, msg))


	stats['all'] = len(remaining)  # all requested paths


	# Pass 0: Filter out invalid jobs
	for d in sorted(remaining):
		try:
			if not looks_like_trial(d):
				remove_and_log(d, 'invalid.nottrial', 'invalid trial.')
		except IOError as e:
			remove_and_log(d, 'invalid.ioerror', 'error reading. (%s)' % e)
	stats['invalid'] = sum(v for (k,v) in stats.items() if k.startswith('invalid.'))


	stats['valid'] = len(remaining)  # all requested paths that are actually trials


	# Pass 1: Detect/mark finished jobs
	stats['finished.old'] = 0
	stats['finished.new'] = 0
	stats['finished.wrong'] = 0
	for d in sorted(remaining):
		if looks_finished(d):
			if is_marked_finished(d):
				remove_and_log(d, 'finished.old', 'finished.')
			else:
				remove_and_log(d, 'finished.new', 'finished. (marker added)')
				mark_finished(d)

			unmark_submitted(d) # ensure d is not detected as "unfinished"

		# trial does not look finished
		else:
			if is_marked_finished(d):
				# Might've been marked by something/somebody else.  Point it out, and leave it alone.
				remove_and_log(d, 'finished.wrong', 'looks incomplete, but is marked as finished!  Skipping.')
	stats['finished'] = sum(v for (k,v) in stats.items() if k.startswith('finished.'))


	# Pass 2: Unfinished but submitted jobs (depends on mode)
	stats['skipped'] = 0
	if mode == Mode.SAFE: # aborts on unfinished jobs
		do_safemode_check(remaining)

	elif mode == Mode.SKIP: # filters out unfinished jobs
		for d in sorted(remaining):
			if is_marked_submitted(d):
				remove_and_log(d, 'skipped', 'unfinished, but already submitted! Skipping. (-s)')

	elif mode == Mode.RESUME: # resubmits unfinished jobs
		pass # handled in submission pass

	elif mode == Mode.CHECK: # stops prior to submission
		for d in sorted(remaining):
			remove_and_log(d, None, 'not finished. (!!!)')
		return stats

	else:
		assert False # complete switch


	# Final pass: Submit all
	stats['submitted.resumed'] = 0
	stats['submitted.new'] = 0
	for d in sorted(remaining):
		# message from slurm submission will be included in print output
		#  to convey either the job id, or the reason for failure
		(success, message) = trial_submit(d)
		if not success:
			logger.warn('%s: failed to submit. (!!!)', d)
			logger.warn('--> %s', message)
			break

		assert success
		if is_marked_submitted(d):
			assert mode == Mode.RESUME
			remove_and_log(d, 'submitted.resumed', 'unfinished, resuming. (-r) (%s)' % message)
		else:
			remove_and_log(d, 'submitted.new', 'submitted! (%s)' % message)
		mark_submitted(d)
	stats['submitted'] = sum(v for (k,v) in stats.items() if k.startswith('submitted.'))
	stats['unprocessed'] = len(remaining) # leftover after sbatch error

	return stats

INDENT = ' '*3

def print_summary(stats, mode):
	if mode == Mode.CHECK:
		print_checkmode_summary(stats)
	else:
		print_general_summary(stats, mode)

def print_summary_header(stats):
	logger.info('')
	logger.info('----SUMMARY----')
	logger.info('%d jobs were requested total.', stats['valid'])

def print_summary_finished(stats):
	logger.info(INDENT*1 + '%d jobs are marked finished.', stats['finished'])
	logger.info(INDENT*2 + '%d are newly marked.', stats['finished.new'])
	if stats['finished.wrong'] > 0:
		logger.info(INDENT*2 + '%d look unfinished! (!!!)', stats['finished.wrong'])

def print_general_summary(stats, mode):
	print_summary_header(stats)
	print_summary_finished(stats)

	if mode == Mode.SKIP:
		logger.info(INDENT*1 + '%d unfinished jobs were skipped. (-s)', stats['skipped'])

	logger.info(INDENT*1 + '%d jobs were submitted.', stats['submitted'])
	if mode == Mode.RESUME:
		logger.info(INDENT*2 + '%d of these were resubmissions. (-r)', stats['submitted.resumed'])

	if stats['unprocessed'] > 0:
		logger.info(INDENT*1 + '%d remain unprocessed after a failed submission. (!!!)', stats['unprocessed'])

def print_checkmode_summary(stats):
	print_summary_header(stats)
	print_summary_finished(stats)

	unfinished_count = stats['valid'] - stats['finished']
	assert unfinished_count >= 0

	warning = ' (!!!)' if unfinished_count > 0 else ''
	logger.info(INDENT*1 + '%d jobs remain unfinished.%s', unfinished_count, warning)

#--------------------------------------
# Trial directory helper methods

def looks_like_trial(path):
	try: return os.path.exists(os.path.join(path, 'INCAR'))
	except (FileNotFoundError, NotADirectoryError):
		return False
	# Note that other IOErrors (e.g. permissions) will still be raised

def looks_finished(path):
	needle = 'Voluntary'
	haystack = os.path.join(path, 'OUTCAR')

	if not os.path.exists(haystack):
		return False

	count = 0
	with open(haystack) as f:
		for line in f:
			if needle in line:
				count += 1

	if count > 1:
		logger.warn('Search term for finished trials may be unreliable?' +
			' (Found phrase \"{}\" multiple times in {}!)', needle, haystack)
	return count > 0

def is_marked_finished(path):
	return os.path.exists(os.path.join(path, FINISHED_FNAME))
def is_marked_submitted(path):
	return os.path.exists(os.path.join(path, SUBMITTED_FNAME))

def mark_finished(path): touch(os.path.join(path, FINISHED_FNAME))
def mark_submitted(path): touch(os.path.join(path, SUBMITTED_FNAME))
def unmark_finished(path): rm_f(os.path.join(path, FINISHED_FNAME))
def unmark_submitted(path): rm_f(os.path.join(path, SUBMITTED_FNAME))

def trial_submit(path):
	''' Returns (success, sbatch_stdout) as (bool, str) '''
	# make argument list
	args = ['sbatch']
	args.extend(shlex.split(SBATCH_FLAGS))
	args.extend(shlex.split(VASP_CMD))

	print(args)
	(out, err) = subprocess.Popen(args, cwd=path, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
	(out, err) = (out.strip(), err.strip())
	words = out.split()
	success = (words[0] == 'Submitted')
	return (success, out)

#--------------------------------------
# Other helper methods

def touch(path):
	''' NOTE: only creates files, does not update timestamps '''
	open(path, 'a').close()

def rm_f(path):
	''' Remove a file. The file need not exist. '''
	try: os.remove(path)
	except FileNotFoundError: pass

#--------------------------------------
# And go!!!!!!!!!

if __name__ == '__main__':
	main()
