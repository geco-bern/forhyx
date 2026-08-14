"""Download the monthly Jones/CRU North Atlantic Oscillation index."""

import argparse
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd


NAO_URL = 'https://crudata.uea.ac.uk/cru/data/nao/nao.dat'
DEFAULT_OUT_PATH = '/storage/capacity/occr_geco/data_2/scratch/fbernhard/FORHYX/'
OUTPUT_FILENAME = 'FORHYX.predictors.nao_monthly.csv'
MISSING_VALUE = -99.99
MONTH_COLUMNS = [
    'jan', 'feb', 'mar', 'apr', 'may', 'jun',
    'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
]
SOURCE_COLUMNS = ['year', *MONTH_COLUMNS, 'annual']

def download_nao_table(url, timeout):
    request = Request(
        url,
        headers={'User-Agent': 'FORHYX scalar-index downloader'},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or 'utf-8'
            return response.read().decode(charset)
    except (HTTPError, URLError, TimeoutError) as exc:
        raise RuntimeError(f'Could not download NAO data from {url}: {exc}') from exc


def parse_nao_table(raw_text):
    """Convert the CRU year-by-month table to a tidy monthly time series."""
    rows = []
    for line_number, line in enumerate(raw_text.splitlines(), start=1):
        fields = line.split()
        if not fields:
            continue
        if len(fields) != len(SOURCE_COLUMNS):
            raise ValueError(
                f'Unexpected NAO row at line {line_number}: expected '
                f'{len(SOURCE_COLUMNS)} fields, found {len(fields)}.'
            )
        try:
            rows.append([int(fields[0]), *map(float, fields[1:])])
        except ValueError as exc:
            raise ValueError(
                f'Invalid numeric value in NAO row at line {line_number}.'
            ) from exc

    if not rows:
        raise ValueError('The downloaded NAO table is empty.')

    wide = pd.DataFrame(rows, columns=SOURCE_COLUMNS)
    if wide['year'].duplicated().any():
        duplicate_years = wide.loc[wide['year'].duplicated(), 'year'].tolist()
        raise ValueError(f'Duplicate years in downloaded NAO data: {duplicate_years}')

    # The thirteenth source value is the annual index. The plotting pipeline
    # consumes monthly predictors, so retain the twelve monthly observations.
    monthly = wide.melt(
        id_vars='year',
        value_vars=MONTH_COLUMNS,
        var_name='month_name',
        value_name='nao',
    )
    month_numbers = {
        name: number for number, name in enumerate(MONTH_COLUMNS, start=1)
    }
    monthly['month'] = monthly['month_name'].map(month_numbers)
    monthly['nao'] = monthly['nao'].replace(MISSING_VALUE, float('nan'))
    monthly['time'] = pd.to_datetime(
        monthly[['year', 'month']].assign(day=1)
    )
    monthly = monthly.sort_values('time', ignore_index=True)
    return monthly[['time', 'year', 'month', 'nao']]


def main():
    monthly_nao = parse_nao_table(
        download_nao_table(NAO_URL, timeout=60)
    )

    out_directory = DEFAULT_OUT_PATH
    out_directory.mkdir(parents=True, exist_ok=True)
    output_path = out_directory / OUTPUT_FILENAME
    temporary_path = output_path.with_suffix(f'{output_path.suffix}.tmp')
    monthly_nao.to_csv(temporary_path, index=False, float_format='%.2f')
    temporary_path.replace(output_path)

    available = monthly_nao.dropna(subset=['nao'])
    print(
        f'Wrote {len(monthly_nao)} monthly rows to {output_path} '
        f'({available.time.min().date()} through {available.time.max().date()}; '
        f'{len(available)} values available).',
        flush=True,
    )


if __name__ == '__main__':
    main()
