#!/usr/bin/env python
"""A script to pull data from the Harvest API into Google Sheets."""

import argparse
import logging
import os
import sys

import gspread_pandas

from bizops import bizops

LOGLEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# This is the ID of a Google spreadsheet in the Catalyst Cooperative GSuite:
WORKBOOK_ID = "19vyQE2sT_uqC8a--6_TODWApk84RZXYTznRkViPkJDs"
AUTH_JSON = os.environ["CATALYST_BIZOPS_CREDENTIALS"]


def parse_main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        dest="endpoints",
        choices=bizops.VALID_HARVEST_ENDPOINTS,
        nargs="+",
        help=f"""List of API endpoints to pull. Must be one or more of:
        {bizops.VALID_HARVEST_ENDPOINTS}.
        """
    )
    parser.add_argument(
        "--loglevel",
        choices=LOGLEVELS,
        help="Set logging level.",
        default="INFO",
    )

    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    args = parse_main(sys.argv)
    logger = logging.getLogger()
    logging.basicConfig(level=args.loglevel)
    gspread_config = gspread_pandas.conf.get_config(file_name=AUTH_JSON)

    for endpoint in args.endpoints:
        logger.info(f"Pulling {endpoint} from Harvest API.")
        df = bizops.get_harvest_df(endpoint)

        logger.info(f"Uploading {len(df)} {endpoint} records to Google Sheets.")
        sheet = gspread_pandas.Spread(
            spread=WORKBOOK_ID,
            sheet=endpoint,
            config=gspread_config,
        )
        sheet.df_to_sheet(df=df, index=False, replace=True)


if __name__ == "__main__":
    sys.exit(main())
