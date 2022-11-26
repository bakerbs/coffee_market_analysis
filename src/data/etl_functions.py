# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np


def process_producer(df, var_name, time_name):
    """ Processes producer data
        df: pandas DataFrame containing raw producer data
        var_name:   variable name to process (production, consumption, openstock, or exports)
        time:name:  crop_year or calendar_year
    """
    x = df
    x.columns = x.iloc[2, :]
    x = x.iloc[3:(x.shape[0]-1), :]
    x = x.dropna(how='all')
    x = x.loc[:, x.columns.notna()]
    x.rename(
        columns={'Crop year': 'country', 'Crop years': 'country', 'Calendar years': 'country'},
        inplace=True)
    x = x.melt(id_vars='country', var_name=time_name, value_name=f'{var_name}_1k_bags')
    x['country'] = x['country'].str.strip()
    x[f'{var_name}_1k_bags'] = x[f'{var_name}_1k_bags'].astype('float', errors='ignore')
    x[f'{var_name}_kg'] = x[f'{var_name}_1k_bags'] * 1000 * 60
    x[f'{var_name}_lb'] = x[f'{var_name}_kg'] * 2.20462262185
    harvest_groups = ['April group', 'July group', 'October group']
    totals = ['Total']
    x['harvest_group'] = \
        np.where(
            x['country'].isin(harvest_groups),
            x['country'].str.replace(r' [Gg]roup', '', regex=True),
            np.NaN
        )
    x['harvest_group'] = x['harvest_group'].ffill()
    if time_name == 'crop_year':
        x['crop_year_beg'] = x['crop_year'].str[0:4].astype('int')
        x['crop_year_end'] = x['crop_year_beg']+1
    hg_rows = x.iloc[:, 0].isin(harvest_groups)
    tot_rows = x.iloc[:, 0].isin(totals)
    x = x.loc[~(hg_rows | tot_rows)]
    if time_name == 'crop_year':
        x = x[
                [
                    'country',
                    'harvest_group',
                    'crop_year',
                    'crop_year_beg',
                    'crop_year_end',
                    f'{var_name}_1k_bags',
                    f'{var_name}_kg',
                    f'{var_name}_lb'
                ]
            ]
    else:
        x = x[
                [
                    'country',
                    'calendar_year',
                    f'{var_name}_1k_bags',
                    f'{var_name}_kg',
                    f'{var_name}_lb'
                ]
            ]
    return (x)


def process_importer(df, var_name, region_map):
    """ Processes importer data
        df: pandas DataFrame containing raw importer data
        var_name:   variable name to process (imports, re_exports)
        region_map:  pandas DataFrame containing regions to map
    """
    x = df
    x.columns = x.iloc[2, :]
    x = x.iloc[3:(x.shape[0]-1), :]
    x = x.dropna(how='all')
    x.rename(columns={'Calendar years': 'country'}, inplace=True)
    x = x.melt(id_vars='country', var_name='calendar_year', value_name=f'{var_name}_1k_bags')
    x['country'] = x['country'].str.strip()
    x[f'{var_name}_1k_bags'] = x[f'{var_name}_1k_bags'].astype('float', errors='ignore')
    eu = ['European Union']
    eu_rows = x.iloc[:, 0].isin(eu)
    totals = ['Total']
    tot_rows = x.iloc[:, 0].isin(totals)
    x = x.loc[~(eu_rows | tot_rows)]
    x = x.merge(region_map, how='left', on='country')
    x['ico_member'] = 'member'

    mask = x.country.isin(['Belgium']) & (x.calendar_year >= 1999)
    B = x.loc[mask, ['calendar_year', f'{var_name}_1k_bags']]

    mask = x.country.isin(['Luxembourg']) & (x.calendar_year >= 1999)
    L = x.loc[mask, ['calendar_year', f'{var_name}_1k_bags']]

    BL = \
        B \
        .merge(L, how='left', on='calendar_year', suffixes=['_B', '_L']) \
        .assign(
            tot=lambda x: x[f'{var_name}_1k_bags_B'] + x[f'{var_name}_1k_bags_L'],
            B_ratio=lambda x: x[f'{var_name}_1k_bags_B']/x.tot
        )

    belg_ratio = BL[f'{var_name}_1k_bags_B'].sum() / BL.tot.sum()

    calced_split = \
        x \
        .loc[
            lambda x: x.country.isin(['Belgium/Luxembourg']),
            ['calendar_year', f'{var_name}_1k_bags']
        ] \
        .assign(
            **{
                f'{var_name}_1k_bags_B': lambda x: x[f'{var_name}_1k_bags'] * belg_ratio,
                f'{var_name}_1k_bags_L': lambda x: x[f'{var_name}_1k_bags'] * (1-belg_ratio)
            }
        )

    B = calced_split[['calendar_year', f'{var_name}_1k_bags_B']] \
        .assign(country='Belgium') \
        .rename(columns={f'{var_name}_1k_bags_B': f'{var_name}_1k_bags'})

    L = calced_split[['calendar_year', f'{var_name}_1k_bags_L']] \
        .assign(country='Luxembourg') \
        .rename(columns={f'{var_name}_1k_bags_L': f'{var_name}_1k_bags'})

    BL = pd.concat([B, L]) \
        .reset_index(drop=True) \
        .loc[:, ['calendar_year', 'country', f'{var_name}_1k_bags']]

    x = \
        x \
        .merge(BL, how='left', on=['calendar_year', 'country'], suffixes=['', '_calc']) \
        .assign(
            **{
                f'{var_name}_1k_bags':
                lambda x: np.where(
                    x[f'{var_name}_1k_bags'].isna(),
                    x[f'{var_name}_1k_bags_calc'],
                    x[f'{var_name}_1k_bags']
                )
            }
        ) \
        .loc[
            lambda x: ~x.country.isin(['Belgium/Luxembourg']),
            [
                'region',
                'country',
                'ico_member',
                'calendar_year',
                f'{var_name}_1k_bags'
            ]
        ]

    x[f'{var_name}_kg'] = x[f'{var_name}_1k_bags'] * 1000 * 60
    x[f'{var_name}_lb'] = x[f'{var_name}_kg'] * 2.20462262185

    col_order = \
        [
            'region',
            'country',
            'ico_member',
            'calendar_year',
            f'{var_name}_1k_bags',
            f'{var_name}_kg',
            f'{var_name}_lb'
        ]

    x = x[col_order]

    return (x)


def process_nonmember(df, var_name):
    """ Processes non-member importer data
        df: pandas DataFrame containing raw importer data
        var_name:   variable name to process (imports, re_exports)
    """
    x = df
    x.columns = x.iloc[2, :]
    x = x.iloc[3:129, :]
    x = x.dropna(how='all')
    x.rename(columns={'Calendar years': 'country'}, inplace=True)
    x = x.melt(id_vars='country', var_name='calendar_year', value_name=f'{var_name}_1k_bags')
    x['country'] = x['country'].str.strip()
    x[f'{var_name}_1k_bags'] = x[f'{var_name}_1k_bags'].astype('float', errors='ignore')
    x[f'{var_name}_kg'] = x[f'{var_name}_1k_bags'] * 1000 * 60
    x[f'{var_name}_lb'] = x[f'{var_name}_kg'] * 2.20462262185
    china_agg = x.iloc[:, 0].isin(["China, People's Republic of"])
    x = x[~china_agg]
    regions = \
        [
            'Africa',
            'Asia & Oceania',
            'Caribbean',
            'Central America & Mexico',
            'Europe',
            'North America',
            'South America'
        ]
    totals = ['Total']
    x['region'] = np.where(x['country'].isin(regions), x['country'], np.NaN)
    x['region'] = x['region'].ffill()
    x['ico_member'] = 'non-member'
    region_rows = x.iloc[:, 0].isin(regions)
    tot_rows = x.iloc[:, 0].isin(totals)
    x = x.loc[~(region_rows | tot_rows)]
    col_order = \
        [
            'region',
            'country',
            'ico_member',
            'calendar_year',
            f'{var_name}_1k_bags',
            f'{var_name}_kg',
            f'{var_name}_lb'
        ]
    x = x[col_order]
    return (x)


# calculated closing stock equals opening stock plus production, less consumption and exports
def close_stock_calc(df, scale):
    """ Rollforward Closing Stock as opening stock plus production, less consumption and exports
        df: pandas DataFrame
        scale: scale for calculation (1k_bags, kg, lb)
    """
    return (
        np.fmax(
            0,
            np.round(
                df[f'openstock_{scale}']
                + df[f'production_{scale}']
                - df[f'consumption_{scale}']
                - df[f'exports_{scale}'],
                2
            )
        )
    )


# closing stock based on time-shifted opening stocks
def close_stock_shift(df, scale):
    """ Calculate Closing Stock as next year's opening stock
        df: pandas DataFrame
        scale: scale for calculation (1k_bags, kg, lb)
    """
    open_shift = \
        df \
        .groupby('country')[f'openstock_{scale}'] \
        .shift(-1)

    return (open_shift)


# stock adjustment -- difference between calculated and shifted closing stocks
def stock_adj(df, scale):
    """ Calculate stock adjustment as the difference between the
        rollforward closing stock and the shifted closing stock

        df: pandas DataFrame
        scale: scale for calculation (1k_bags, kg, lb)
    """
    return (close_stock_shift(df, scale) - close_stock_calc(df, scale))
