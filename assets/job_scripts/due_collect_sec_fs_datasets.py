"""
This module is made to collect the latest available SEC Financial Statement dataset
"""

from datetime import date
import sys
from sec_fs_dataset_collector.collect_sec_fs_datasets import handle_quarter
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["source_url", "s3_bucket", "s3_prefix_dest", "year", "quarter"])

if not args.get("year") or not args.get("quarter"):
    handle_quarter(year=str(date.today().year), quarter=str(date.today().month//3 + 1), s3_bucket=args['s3_bucket'], s3_prefix_dest=args['S3_prefix_dest'], source_url=args['source_url'])
else:
    handle_quarter(year=args["year"], quarter=args["quarter"], s3_bucket=args["s3_bucket"], s3_prefix_dest=args["s3_prefix_dest"], source_url=args["source_url"])