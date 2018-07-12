#!/usr/bin/env python

import logging
import os
import subprocess


class PandnaFileError(OSError):
    pass


class PandnaShellError(subprocess.SubprocessError):
    pass


class ShellOperator:
    def __init__(self, log_txt=None, quiet=False, format_log=False,
                 logger=None, executable='/bin/bash'):
        self.logger = logger or logging.getLogger(__name__)
        self.executable = executable
        self.log_txt = log_txt
        self.quiet = quiet
        self.returncode = None
        if log_txt:
            if format_log:
                self.logger.debug('Write a log file: {}'.format(log_txt))
                with open(log_txt, 'w') as f:
                    f.write('# shell: {0}{1}'.format(executable, os.linesep))
            self.post_proc = (
                ' >> {} 2>&1'.format(log_txt) if quiet
                else ' 2>&1 | tee -a {}'.format(
                    log_txt
                ) + ' && exit ${PIPESTATUS[-2]}'
            )
        else:
            self.post_proc = ' > /dev/null 2>&1' if quiet else ''

    def run(self, args, target_files=None, target_validator=None, check=True,
            cwd=None, prompt=None):
        pp = prompt or '[{}] $'.format(os.getcwd())
        for a in self._args2list(args):
            cmd = a + self.post_proc
            self.logger.debug('shell:{0}{1} {2}'.format(os.linesep, pp, cmd))
            if self.log_txt:
                with open(self.log_txt, 'a') as f:
                    f.write('{0}{1} {2}{0}'.format(os.linesep, pp, cmd))
            self.returncode = subprocess.run(
                cmd, executable=self.executable, stdout=None, stderr=None,
                shell=True, check=check, cwd=cwd
            ).returncode
        self._validate_targets(
            files=target_files, func=target_validator, clean_if_failed=True
        )

    def run_parallel(self, args, target_files=None, target_validator=None,
                     check=True, cwd=None, prompt=None):
        pp = prompt or '[{}] $'.format(os.getcwd())
        arg_list = self._args2list(args)
        if self.log_txt:
            tmp_log_txts = [
                self.log_txt + '.{}'.format(i)
                for i, a in enumerate(arg_list)
            ]
            cmds = [
                (
                    a + ' >> {} 2>&1'.format(l)
                    if self.quiet else
                    a + ' 2>&1 | tee -a {}'.format(l) +
                    ' && exit ${PIPESTATUS[-2]}'
                )
                for a, l in zip(arg_list, tmp_log_txts)
            ]
            for c, l in zip(cmds, tmp_log_txts):
                with open(l, 'w') as f:
                    f.write('{0}{1} {2}{0}'.format(os.linesep, pp, c))
        else:
            cmds = [
                a + (' > /dev/null 2>&1' if self.quiet else '')
                for a in arg_list
            ]
        self.logger.debug('shell:{}'.format(
            ''.join([(os.linesep + pp + ' ' + c) for c in cmds])
        ))
        procs = [
            subprocess.Popen(
                c, executable=self.executable, stdout=None, stderr=None,
                shell=True, cwd=cwd
            )
            for i, c in enumerate(cmds)
        ]
        try:
            for p, l in zip(procs, tmp_log_txts):
                r = p.communicate()
                self.logger.debug('p.communicate(): {}'.format(r))
                with open(self.log_txt, 'a') as lt:
                    with open(l, 'r') as tlt:
                        lt.write(tlt.read())
                os.remove(l)
                self.returncode = p.returncode
                if p.returncode != 0:
                    raise PandnaShellError(
                        'Command \'{0}\' returned non-zero exit status '
                        '{1}.'.format(p.args, p.returncode)
                    )
        except PandnaShellError as e:
            for p, l in zip(procs, tmp_log_txts):
                if not p.returncode:
                    p.kill()
                if os.path.isfile(l):
                    os.remove(l)
            raise e
        else:
            self._validate_targets(
                files=target_files, func=target_validator, clean_if_failed=True
            )

    @staticmethod
    def _args2list(args):
        if isinstance(args, list):
            return args
        elif any([isinstance(args, c) for c in [tuple, set, dict]]):
            return list(args)
        else:
            return [args]

    def _validate_targets(self, files=None, func=None, clean_if_failed=True):
        if files:
            f_all = self._args2list(files)
            f_found = {p for p in f_all if os.path.exists(p)}
            f_not_found = set(f_all).difference(f_found)
            if f_not_found:
                if clean_if_failed and f_found:
                    self.logger.debug(
                        'files removed: {}'.format(', '.join(f_found))
                    )
                    [os.remove(p) for p in f_found]
                else:
                    pass
                raise PandnaShellError(
                    'target not found: {}'.format(', '.join(f_not_found))
                )
            elif func:
                f_validated = {p for p in f_found if func(p)}
                f_not_validated = set(f_found).difference(f_validated)
                if f_not_validated:
                    if clean_if_failed:
                        self.logger.debug(
                            'files removed: {}'.format(', '.join(f_found))
                        )
                        [os.remove(p) for p in f_found]
                    else:
                        pass
                    raise PandnaShellError(
                        'target not validated with {0}: {1}'.format(
                            func, ', '.join(f_not_validated)
                        )
                    )
                else:
                    self.logger.debug(
                        'target validated with {0}: {1}'.format(
                            func, ', '.join(f_validated)
                        )
                    )
            else:
                self.logger.debug(
                    'target validated: {}'.format(', '.join(f_found))
                )
        else:
            pass
