#!/usr/bin/env python
#
# Pandas-based Data Frame Handlers DNA-sequencing
# https://github.com/dceoy/pandna

import io
import pandas as pd
from .base_bio_data_frame import BaseBioDataFrame, BioDataFrameError


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
            for s in self.run_and_parse_subprocess(args=args):
                self._load_vcf_line(string=s)
        self.df = self.df.reset_index(drop=True)

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
