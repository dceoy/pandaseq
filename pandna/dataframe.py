#!/usr/bin/env python

import io
import os
import logging
import re
import subprocess
import pandas as pd
from . import PandnaFileError, PandnaShellError


class BaseDataFrame:
    def __init__(self, path, logger=None, supported_exts=None):
        self.logger = logger or logging.getLogger(__name__)
        self.path = path
        self.df = None      # mutable
        if os.path.isfile(path):
            self.logger.info('table file path: {}'.format(path))
        else:
            raise PandnaFileError('file not found: {}'.format(path))
        ext = os.path.splitext(self.path)[1]
        if supported_exts is None or ext in supported_exts:
            self.logger.info('file extension: {}'.format(ext))
        else:
            raise PandnaFileError('unsupported file extension: {}'.format(ext))

    def load_csv(self, path, **kwargs):
        self.df = pd.read_csv(path, **kwargs)

    def load_tsv(self, path, **kwargs):
        self.df = pd.read_table(path, **kwargs)


class SamDataFrame(BaseDataFrame):
    def __init__(self, path, logger=None, max_n_opt_cols=20, samtools=None,
                 n_thread=1):
        super().__init__(
            path=path, logger=logger, supported_exts=['.sam', '.bam', '.cram']
        )
        self.samtools = samtools or 'samtools'
        self.n_thread = n_thread
        optional_cols = ['OPT{}'.format(i + 1) for i in range(max_n_opt_cols)]
        self.mandatory_cols = [
            'QNAME', 'FLAG', 'RNAME', 'POS', 'MAPQ', 'CIGAR', 'RNEXT',
            'PNEXT', 'TLEN', 'SEQ', 'QUAL'
        ]
        self.col_names = [*self.mandatory_cols, *optional_cols]
        self.col_dtypes = {
            'QNAME': str, 'FLAG': int, 'RNAME': str, 'POS': int, 'MAPQ': int,
            'CIGAR': str, 'RNEXT': str, 'PNEXT': int, 'TLEN': int, 'SEQ': str,
            'QUAL': str, **{k: str for k in optional_cols}
        }
        self.header = []                                        # mutable
        self.df = pd.DataFrame(                                 # mutable
            [], columns=self.col_names, dtype=self.col_dtypes
        )

    def load_sam(self):
        if self.path.endswith('.sam'):
            with open(self.path, 'r') as f:
                for s in f:
                    self._parse_sam_line(string=s)
            self._drop_na_cols(self)
        else:
            args = (
                [self.samtools, 'view'] +
                (['-@', str(self.n_thread)] if self.n_thread > 1 else []) +
                ['-h', self.path]
            )
            with subprocess.Popen(args=args, stdout=subprocess.PIPE) as p:
                for s in iter(p.stdout.readline(), ''):
                    self._parse_sam_line(string=s)
            if p.returncode != 0:
                raise PandnaShellError(
                    'Subprocess \'{0}\' returned non-zero exit status '
                    '{1}.'.format(' '.join(p.args), p.returncode)
                )
            else:
                self._drop_na_cols()

    def _parse_sam_line(self, string):
        if re.match(r'@[A-Z]{1}', string):
            self.header.append(string)
        else:
            self.df = self.df.append(
                pd.read_table(
                    io.StringIO(string), header=None,
                    columns=self.col_names, dtype=self.col_dtypes
                )
            )

    def _drop_na_cols(self):
        self.df = self.df.pipe(
            lambda d: pd.concat(
                d[self.mandatory_cols],
                d[
                    [c for c in d.columns if c in self.mandatory_cols]
                ].dropna(axis=1),
                axis=1
            )
        )


class VcfDataFrame(BaseDataFrame):
    def __init__(self, path, logger=None):
        super().__init__(path=path, logger=logger)

    def read_vcf(self, **kwargs):
        with open(self.path, 'r') as f:
            lines = [l for l in f]
        self.df = pd.read_table(
            io.StringIO(''.join([l for l in lines if not l.startswith('##')])),
            dtype={
                '#CHROM': str, 'POS': int, 'ID': str, 'REF': str,
                'ALT': str, 'QUAL': str, 'FILTER': str, 'INFO': str
            }
        ).rename(columns={'#CHROM': 'CHROM'})
