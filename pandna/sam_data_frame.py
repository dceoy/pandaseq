#!/usr/bin/env python
#
# Pandas-based Data Frame Handlers DNA-sequencing
# https://github.com/dceoy/pandna

import io
import logging
import re
import subprocess
import pandas as pd
from .base_bio_data_frame import BaseBioDataFrame


class SamDataFrame(BaseBioDataFrame):
    def __init__(self, path, samtools='samtools', n_thread=1):
        super().__init__(path=path, supported_exts=['.sam', '.bam', '.cram'])
        self.logger = logging.getLogger(__name__)
        self.samtools = samtools
        self.n_thread = n_thread
        self.fixed_cols = [
            'QNAME', 'FLAG', 'RNAME', 'POS', 'MAPQ', 'CIGAR', 'RNEXT', 'PNEXT',
            'TLEN', 'SEQ', 'QUAL'
        ]
        self.fixed_col_dtypes = {
            'QNAME': str, 'FLAG': int, 'RNAME': str, 'POS': int, 'MAPQ': int,
            'CIGAR': str, 'RNEXT': str, 'PNEXT': int, 'TLEN': int, 'SEQ': str,
            'QUAL': str
        }
        self.header = []
        self.detected_cols = []
        self.detected_col_dtypes = {}

    def load(self):
        if self.path.endswith('.sam'):
            with open(self.path, 'r') as f:
                for s in f:
                    self._load_sam_line(string=s)
        else:
            th_args = (['-@', str(self.n_thread)] if self.n_thread > 1 else [])
            args = [self.samtools, 'view', *th_args, '-h', self.path]
            with subprocess.Popen(args=args, stdout=subprocess.PIPE) as p:
                while True:
                    line = p.stdout.readline().decode('utf-8')
                    if line:
                        self._load_sam_line(string=line)
                    elif p.poll() is not None:
                        break
                    else:
                        pass
                if p.returncode:
                    raise subprocess.CalledProcessError(
                        returncode=p.returncode, cmd=p.args, stderr=p.stderr
                    )

    def _load_sam_line(self, string):
        if re.match(r'@[A-Z]{1}', string):
            self.header.append(string.strip())
        else:
            if not self.detected_cols:
                n_fixed_cols = len(self.fixed_cols)
                n_detected_cols = string.count('\t') + 1
                self.detected_cols = self.fixed_cols + (
                    [
                        'OPT{}'.format(i)
                        for i in range(n_detected_cols - n_fixed_cols)
                    ] if n_detected_cols > n_fixed_cols else []
                )
                self.detected_col_dtypes = {
                    k: (self.fixed_col_dtypes.get(k) or str)
                    for k in self.detected_cols
                }
            self.df = self.df.append(
                pd.read_table(
                    io.StringIO(string), header=None, names=self.detected_cols,
                    dtype=self.detected_col_dtypes
                )
            )


class SamtoolsFlagstatDataFrame(BaseBioDataFrame):
    def __init__(self, path):
        super().__init__(path=path)
        self.cols = ['qc_passed', 'qc_failed', 'read']
        self.col_dtypes = {'read': str, 'qc_passed': int, 'qc_failed': int}

    def load(self):
        with open(self.path, 'r') as f:
            for s in f:
                self._load_samtools_flagstat_line(string=s)

    def _load_samtools_flagstat_line(self, string):
        self.df = self.df.append(
            pd.read_table(
                io.StringIO(
                    string.replace(' + ', '\t', 1).replace(' ', '\t', 1)
                ),
                header=None, names=self.cols, dtype=self.col_dtypes
            )[['read', 'qc_passed', 'qc_failed']]
        )
