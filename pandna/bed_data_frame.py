#!/usr/bin/env python
#
# Pandas-based Data Frame Handlers DNA-sequencing
# https://github.com/dceoy/pandna

import io
import pandas as pd
from .base_bio_data_frame import BaseBioDataFrame


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
