#!/usr/bin/env python

import io
import os
import logging
import pandas as pd
from . import PandnaError


class BaseDataFrame:
    def __init__(self, path, logger=None):
        self.path = path
        self.logger = logger or logging.getLogger(__name__)
        if os.path.isfile(path):
            logger.info('table file path: {}'.format(path))
        else:
            raise PandnaError('file not found: {}'.format(path))

    def read_csv(self, **kwargs):
        self.df = pd.read_csv(self.path, **kwargs)

    def read_table(self, **kwargs):
        self.df = pd.read_table(self.path, **kwargs)


class SamDataFrame(BaseDataFrame):
    def __init__(self, path, mode='rb', logger=None):
        super().__init__(path=path, logger=logger)


class VcfDataFrame(BaseDataFrame):
    def __init__(self, path, logger=None):
        super().__init__(path=path, logger=logger)

    def read_vcf(self, **kwargs):
        with open(self.path, 'r') as f:
            lines = [l for l in f]
        self.df = pd.read_table(
            io.StringIO(
                str.join(
                    os.linesep,
                    [l for l in lines if not l.startswith('##')]
                )
            ),
            dtype={
                '#CHROM': str, 'POS': int, 'ID': str, 'REF': str,
                'ALT': str, 'QUAL': str, 'FILTER': str, 'INFO': str
            }
        ).rename(columns={'#CHROM': 'CHROM'})
