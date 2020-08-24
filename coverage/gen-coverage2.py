#!/usr/bin/env python3

# 2017, Georg Sauthoff <mail@gms.tf>, GPLv3

import argparse
import logging
import os
import subprocess
import shutil
import sys

sys.path.insert(0, os.path.dirname(__file__))
import filterbr

log = logging.getLogger(__name__)

#ex_path = [ '/usr/include/*', 'unittest/*', 'lib*/*', '*@exe/*', 'example/*' ]
ex_path = [ '/usr/include/*', '/usr/local/include/*', '/usr/lib/*', '*/bazel_out/*', '*/k8-dbg/*', 'test/*', '*/test/*', '*/external/*' , '*/include/*' , '*/thirdparties/*' , '*/snapshotcloneserver/*' ]

brflag = ['--rc', 'lcov_branch_coverage=1']

lcov = 'lcov'
base = os.path.abspath('.')

cov_init_raw = 'coverage_init_raw.info'
cov_post_raw = 'coverage_post_raw.info'
cov_init     = 'coverage_init.info'
cov_post     = 'coverage_post.info'
cov_br       = 'coverage.info'
cov          = 'coverage.info'
report_dir   = 'coverage'

def setup_logging():
  log_format      = '{rel_secs:6.1f} {lvl}  {message}'
  log_date_format = '%Y-%m-%d %H:%M:%S'

  class Relative_Formatter(logging.Formatter):
    level_dict = { 10 : 'DBG',  20 : 'INF', 30 : 'WRN', 40 : 'ERR',
        50 : 'CRI' }
    def format(self, rec):
      rec.rel_secs = rec.relativeCreated/1000.0
      rec.lvl = self.level_dict[rec.levelno]
      return super(Relative_Formatter, self).format(rec)

  log = logging.getLogger() # root logger
  log.setLevel(logging.DEBUG)
  ch = logging.StreamHandler()
  ch.setLevel(logging.INFO)
  ch.setFormatter(Relative_Formatter(log_format, log_date_format, style='{'))
  log.addHandler(ch)

def mk_arg_parser():
  p = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='Do some stuff',
        epilog='...')
  p.add_argument('--html', action='store_true', default=True,
      help='generate HTML report (default: on)')
  p.add_argument('--no-html', dest='html', action='store_false',
      help='disable html report generation')
  p.add_argument('--filter-br', action='store_true', default=True,
      help='filter branch coverage data (default: on)')
  p.add_argument('--no-filter-br', dest='filter_br', action='store_false',
      help='disable branch filtering')
  return p

def parse_args(*a):
  arg_parser = mk_arg_parser()
  args = arg_parser.parse_args(*a)
  global cov_br
  if args.filter_br:
    cov_br = 'coverage_br.info'
  return args

def run(*args, **kw):
  log.info('Executing: ' + ' '.join(map(lambda s:"'"+s+"'", args[0])))
  return subprocess.run(*args, **kw, check=True)


def main(args):
  run([lcov, '--directory',  'src', '--capture', '-o', cov_post_raw] + brflag )
  run([lcov, '--directory',  'src', '--capture', '--initial', '-o', cov_init_raw])

  for i, o in [ (cov_init_raw, cov_init), (cov_post_raw, cov_post) ]:
    run([lcov, '--remove', i] + ex_path + [ '-o', o] + brflag)

  run([lcov, '-a', cov_init, '-a', cov_post, '-o', cov_br] + brflag)

  if args.filter_br:
    log.info('Filtering branch coverage data ({} -> {})'.format(cov_br, cov))
    with open(cov, 'w') as f:
      filterbr.filter_lcov_trace_file(cov_br, f)

  if args.html:
    shutil.rmtree(report_dir, ignore_errors=True)
    run(['genhtml', cov, '--branch-coverage', '--ignore-errors', 'source', '-o', report_dir])

  return 0

if __name__ == '__main__':
  setup_logging()
  args = parse_args()
  sys.exit(main(args))



