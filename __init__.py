#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch county-level COVID-19 data for the state of Texas.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any, List
from meerschaum.config._paths import PLUGINS_TEMP_RESOURCES_PATH
import datetime
import pathlib

__version__ = '0.1.0'
TMP_PATH = PLUGINS_TEMP_RESOURCES_PATH / 'TX-covid_data'
XLSX_URL = "https://www.dshs.texas.gov/coronavirus/TexasCOVID19DailyCountyCaseCountData.xlsx"
XLSX_PATH = TMP_PATH / 'Texas COVID-19 Case Count Data by County.xlsx'
COUNTIES_PATH = pathlib.Path(__file__).parent / 'counties.csv'
required = ['requests', 'python-dateutil', 'pandas', 'duckdb', 'openpyxl']

def register(pipe: meerschaum.Pipe, **kw):
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.prompt import prompt, yes_no
    while True:
        fips_str = prompt("Please enter a list of FIPS codes separated by commas:")
        fips = fips_str.replace(' ', '').split(',')

        valid = True
        for f in fips:
            if not f.startswith("48"):
                warn("All FIPS codes must begin with 48 (prefix for the state of Texas).", stack=False)
                valid = False
                break
        if not valid:
            continue

        question = "Is this correct?"
        for f in fips:
            question += f"\n  - {f}"
        question += '\n'

        if not fips or not yes_no(question):
            continue
        break

    return {
        'columns': {
            'datetime': 'date',
            'id': 'fips',
            'value': 'cases'
        },
        'TX-covid': {
            'fips': fips,
        },
    }


def fetch(
        pipe: meerschaum.Pipe,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        debug: bool = False,
        **kw
    ):
    import pandas as pd
    from meerschaum.utils.misc import wget
    from dateutil import parser
    import datetime
    import duckdb
    import textwrap
    from meerschaum.utils.debug import dprint
    TMP_PATH.mkdir(exist_ok=True, parents=True)
    fips = pipe.parameters['TX-covid']['fips']
    fips_where = "'" + "', '".join(fips) + "'"
    counties_df = pd.read_csv(COUNTIES_PATH, dtype={'fips': str, 'counties': str, 'state': str})
    dtypes = {
        'date': 'datetime64[ms]',
        'county': str,
        'fips': str,
        'cases': int,
    }
    begin = begin if begin is not None else (pipe.get_sync_time(debug=debug) if end is None else None)
    sheets_to_read = None if begin is None else [f'Cases by County {year}' for year in get_years(begin, end)]
    wget(XLSX_URL, XLSX_PATH, debug=debug)
    sheets_dfs = pd.read_excel(XLSX_PATH, sheet_name=None, skiprows=[0], header=1, nrows=254)
    df = pd.concat([df for k, df in sheets_dfs.items()], join='outer', axis=1)
    data = {'date': [], 'county': [], 'cases': [],}
    counties = list(df.iloc[:,0])

    if end is not None and begin is not None and end < begin:
        begin = end - datetime.timedelta(days=1)
    for col in df.columns[1:]:
        try:
            date = parser.parse(str(col))
        except parser._parser.ParserError:
            continue
        except TypeError:
            print(f'{col=}')
        if begin is not None and date < begin:
            continue
        if end is not None and date > end:
            break
        for i, county in enumerate(counties):
            data['date'].append(date)
            data['county'].append(county)
            data['cases'].append(df[col][i])

    clean_df = pd.DataFrame(data).astype({col: typ for col, typ in dtypes.items() if col in data})
    if debug:
        print(clean_df)
    query = textwrap.dedent(f"""
    SELECT
        CAST(d.date AS DATE) AS date, 
        c.fips,
        c.county,
        d.cases AS cases
    FROM clean_df AS d
    INNER JOIN counties_df AS c ON c.county = d.county
    WHERE c.fips IN ({fips_where})
        AND d.cases IS NOT NULL
        AND d.date IS NOT NULL"""
    )
    if begin is not None:
        begin -= datetime.timedelta(days=2)
        query += f"\n    AND CAST(d.date AS DATE) >= CAST('{begin}' AS DATE)"
    if end is not None:
        query += f"\n    AND CAST(d.date AS DATE) <= CAST('{end}' AS DATE)"
    if debug:
        print(query)

    joined_df = duckdb.query(query).df()[dtypes.keys()].astype(dtypes)
    if debug:
        print(joined_df)
    return joined_df


def get_years(
        begin: datetime.datetime,
        end: Optional[datetime.datetime] = None,
    ) -> List[int]:
    """
    From the beginning datetime, get the years (as ints) that we are going to read.
    """
    import datetime
    max_year = datetime.datetime.utcnow().year if end is None else end.year
    return list(range(begin.year, max_year + 1))
