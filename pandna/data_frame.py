#!/usr/bin/env python
#
# Pandas-based Data Frame Handlers DNA-sequencing
# https://github.com/dceoy/pandna

from abc import ABCMeta, abstractmethod
import io
import os
import logging
import re
import subprocess
import pandas as pd


class PandnaRuntimeError(RuntimeError):
    pass


class BaseDF(object, metaclass=ABCMeta):
    def __init__(self, path, supported_exts=list()):
        if os.path.isfile(path):
            self.path = path
        else:
            raise PandnaRuntimeError('file not found: {}'.format(path))
        exts = [x for x in supported_exts if path.endswith(x)]
        if not supported_exts:
            self.ext = None
        elif exts:
            self.ext = exts[0]
        else:
            raise PandnaRuntimeError('invalid file extension: {}'.format(path))
        self.df = pd.DataFrame()

    @abstractmethod
    def load(self):
        pass

    def load_and_output_df(self):
        self.load()
        return self.df

    @staticmethod
    def drop_na_col(df, fixed_cols=[]):
        return pd.concat(
            df[fixed_cols],
            df[[c for c in df.columns if c not in fixed_cols]].dropna(axis=1),
            axis=1
        )


class SamDF(BaseDF):
    def __init__(self, path, samtools='samtools', n_thread=1,
                 max_n_opt_cols=20):
        super().__init__(path=path, supported_exts=['.sam', '.bam', '.cram'])
        self.logger = logging.getLogger(__name__)
        self.samtools = samtools
        self.n_thread = n_thread
        self.fixed_cols = [
            'QNAME', 'FLAG', 'RNAME', 'POS', 'MAPQ', 'CIGAR', 'RNEXT', 'PNEXT',
            'TLEN', 'SEQ', 'QUAL'
        ]
        optional_cols = ['OPT{}'.format(i) for i in range(max_n_opt_cols)]
        self.cols = [*self.fixed_cols, *optional_cols]
        self.col_dtypes = {
            'QNAME': str, 'FLAG': int, 'RNAME': str, 'POS': int, 'MAPQ': int,
            'CIGAR': str, 'RNEXT': str, 'PNEXT': int, 'TLEN': int, 'SEQ': str,
            'QUAL': str, **{k: str for k in optional_cols}
        }
        self.header = []

    def load(self):
        if self.path.endswith('.sam'):
            with open(self.path, 'r') as f:
                [self._load_sam_line(string=s) for s in f]
            self.df = self.drop_na_col(self.df, fixed_cols=self.fixed_cols)
        else:
            th_args = (['-@', str(self.n_thread)] if self.n_thread > 1 else [])
            args = [self.samtools, 'view', *th_args, '-h', self.path]
            with subprocess.Popen(args=args, stdout=subprocess.PIPE) as p:
                for s in iter(p.stdout.readline(), ''):
                    self._load_sam_line(string=s)
            if p.returncode != 0:
                raise subprocess.SubprocessError(
                    'Subprocess \'{0}\' returned non-zero exit status '
                    '{1}.'.format(' '.join(p.args), p.returncode)
                )
            else:
                self.df = self.drop_na_col(self.df, fixed_cols=self.fixed_cols)

    def _load_sam_line(self, string):
        if re.match(r'@[A-Z]{1}', string):
            self.header.append(string.strip())
        else:
            self.df = self.df.append(
                pd.read_table(
                    io.StringIO(string), header=None, names=self.cols,
                    dtype=self.col_dtypes
                )
            )


class SamtoolsFlagstatDF(BaseDF):
    def __init__(self, path):
        super().__init__(path=path)
        self.cols = ['qc_passed', 'qc_failed', 'read']
        self.col_dtypes = {'read': str, 'qc_passed': int, 'qc_failed': int}

    def load(self):
        with open(self.path, 'r') as f:
            [self._load_samtools_flagstat_line(string=s) for s in f]

    def _load_samtools_flagstat_line(self, string):
        self.df = self.df.append(
            pd.read_table(
                io.StringIO(
                    string.replace(' + ', '\t', 1).replace(' ', '\t', 1)
                ),
                header=None, names=self.cols, dtype=self.col_dtypes
            )[['read', 'qc_passed', 'qc_failed']]
        )


class VcfDF(BaseDF):
    def __init__(self, path, bcftools='bcftools', n_thread=1,
                 max_n_opt_cols=20):
        super().__init__(path=path, supported_exts=['.vcf', '.vcf.gz', '.bcf'])
        self.bcftools = bcftools
        self.n_thread = n_thread
        self.fixed_cols = [
            '#CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO',
            'FORMAT'
        ]
        optional_cols = ['SAMPLE{}'.format(i) for i in range(max_n_opt_cols)]
        self.cols = [*self.fixed_cols, *optional_cols]
        self.col_dtypes = {
            '#CHROM': str, 'POS': int, 'ID': str, 'REF': str, 'ALT': str,
            'QUAL': str, 'FILTER': str, 'INFO': str,
            **{k: str for k in optional_cols}
        }
        self.header = []
        self.samples = []

    def load(self):
        if self.path.endswith('.vcf'):
            with open(self.path, 'r') as f:
                [self._load_vcf_line(string=s) for s in f]
            self.df = self.drop_na_col(self.df, fixed_cols=self.fixed_cols)
        else:
            th_args = (
                ['--threads', str(self.n_thread)] if self.n_thread > 1 else []
            )
            args = [self.bcftools, 'view', *th_args, self.path]
            with subprocess.Popen(args=args, stdout=subprocess.PIPE) as p:
                for s in iter(p.stdout.readline(), ''):
                    self._load_vcf_line(string=s)
            if p.returncode != 0:
                raise subprocess.SubprocessError(
                    'Subprocess \'{0}\' returned non-zero exit status '
                    '{1}.'.format(' '.join(p.args), p.returncode)
                )
            else:
                self.df = self.drop_na_col(self.df, fixed_cols=self.fixed_cols)

    def _load_vcf_line(self, string):
        if string.startswith('##'):
            self.header.append(string.strip())
        elif string.startswith('#CHROM'):
            items = string.strip().split('\t')
            if items == self.fixed_cols[:len(items)]:
                self.samples = [s for s in items if s not in self.fixed_cols]
            else:
                raise PandnaRuntimeError('invalid VCF columns')
        else:
            self.df = self.df.append(
                pd.read_table(
                    io.StringIO(string), header=None, names=self.cols,
                    dtype=self.col_dtypes
                )
            )
