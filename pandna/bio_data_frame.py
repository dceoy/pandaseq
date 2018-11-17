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


class BioDataFrameError(RuntimeError):
    pass


class BaseBioDataFrame(object, metaclass=ABCMeta):
    def __init__(self, path, supported_exts=[]):
        if os.path.isfile(path):
            self.path = path
        else:
            raise BioDataFrameError('file not found: {}'.format(path))
        exts = [x for x in supported_exts if path.endswith(x)]
        if not supported_exts:
            self.ext = None
        elif exts:
            self.ext = exts[0]
        else:
            raise BioDataFrameError('invalid file extension: {}'.format(path))
        self.df = pd.DataFrame()

    @abstractmethod
    def load(self):
        pass

    def load_and_output_df(self):
        self.load()
        return self.df

    def write_df(self, path, mode='w', **kwargs):
        if self.header:
            with open(path, mode=mode) as f:
                for h in self.header:
                    f.write(h + os.linesep)
        self.df.to_csv(path, mode=('a' if self.header else 'w'), **kwargs)


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
                for s in iter(p.stdout.readline(), ''):
                    self._load_sam_line(string=s)
            if p.returncode != 0:
                raise subprocess.SubprocessError(
                    'Subprocess \'{0}\' returned non-zero exit status '
                    '{1}.'.format(' '.join(p.args), p.returncode)
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


class VcfDataFrame(BaseBioDataFrame):
    def __init__(self, path, bcftools='bcftools', n_thread=1):
        super().__init__(path=path, supported_exts=['.vcf', '.vcf.gz', '.bcf'])
        self.bcftools = bcftools
        self.n_thread = n_thread
        self.fixed_cols = [
            '#CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO',
            'FORMAT'
        ]
        self.fixed_col_dtypes = {
            '#CHROM': str, 'POS': int, 'ID': str, 'REF': str, 'ALT': str,
            'QUAL': str, 'FILTER': str, 'INFO': str
        }
        self.header = []
        self.samples = []
        self.detected_cols = []
        self.detected_col_dtypes = {}

    def load(self):
        if self.path.endswith('.vcf'):
            with open(self.path, 'r') as f:
                for s in f:
                    self._load_vcf_line(string=s)
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

    def _load_vcf_line(self, string):
        if string.startswith('##'):
            self.header.append(string.strip())
        elif string.startswith('#CHROM'):
            items = string.strip().split('\t')
            if items[:len(self.fixed_cols)] == self.fixed_cols:
                self.samples = [s for s in items if s not in self.fixed_cols]
                n_fixed_cols = len(self.fixed_cols)
                n_detected_cols = len(items)
                self.detected_cols = self.fixed_cols + (
                    [
                        'SAMPLE{}'.format(i)
                        for i in range(n_detected_cols - n_fixed_cols)
                    ] if n_detected_cols > n_fixed_cols else []
                )
                self.detected_col_dtypes = {
                    k: (self.fixed_col_dtypes.get(k) or str)
                    for k in self.detected_cols
                }
            else:
                raise BioDataFrameError('invalid VCF columns')
        else:
            self.df = self.df.append(
                pd.read_table(
                    io.StringIO(string), header=None, names=self.detected_cols,
                    dtype=self.detected_col_dtypes
                )
            )


class BedDataFrame(BaseBioDataFrame):
    def __init__(self, path, opt_cols=[]):
        super().__init__(path=path, supported_exts=['.bed', '.txt', '.tsv'])
        self.fixed_cols = ['chrom', 'chromStart', 'chromEnd']
        self.opt_cols = opt_cols or [
            'name', 'score', 'strand', 'thickStart', 'thickEnd', 'itemRgb',
            'blockCount', 'blockSizes', 'blockStarts'
        ]
        self.fixed_col_dtypes = {
            'chrom': str, 'chromStart': int, 'chromEnd': int, 'name': str,
            'score': int, 'strand': str, 'thickStart': int, 'thickEnd': int,
            'itemRgb': str, 'blockCount': int, 'blockSizes': int,
            'blockStarts': int
        }
        self.header = []
        self.detected_cols = []
        self.detected_col_dtypes = {}

    def load(self):
        with open(self.path, 'r') as f:
            for s in f:
                self._load_bed_line(string=s)

    def _load_bed_line(self, string):
        if string.startswith(('browser', 'track')):
            self.header.append(string.strip())
        else:
            if not self.detected_cols:
                self.detected_cols = [
                    *self.fixed_cols, *self.opt_cols
                ][:(string.count('\t') + 1)]
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
