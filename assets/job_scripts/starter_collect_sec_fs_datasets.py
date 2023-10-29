"""
This module is made to catchup on SEC Financial Statements since Q1 2009
"""

from datetime import date
import sys
from sec_fs_dataset_collector.collect_sec_fs_datasets import handle_quarter
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["source_url", "s3_bucket", "s3_prefix_dest"])

INITIAL_YEAR = 2009
CURRENT_YEAR = date.today().year

for year in range(INITIAL_YEAR, CURRENT_YEAR):
    for quarter in range(1, 5):
        handle_quarter(year=str(year), quarter=str(quarter), s3_bucket=args['s3_bucket'], s3_prefix_dest=args['S3_prefix_dest'], source_url=args['source_url'])

for quarter in range(1, date.today().month//3 + 2):
    handle_quarter(year=str(CURRENT_YEAR), quarter=str(quarter), s3_bucket=args['s3_bucket'], s3_prefix_dest=args['S3_prefix_dest'], source_url=args['source_url'])