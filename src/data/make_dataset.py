# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
from src.data.etl_functions import *
import pandas as pd
import numpy as np
import glob
import warnings
warnings.filterwarnings('ignore')


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('external_filepath', type=click.Path(exists=True))
@click.argument('interim_filepath', type=click.Path())
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, external_filepath, interim_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

    # read raw data files
    source_files = pd.DataFrame({'file_path' : sorted(glob.glob(f'{input_filepath}/*.xlsx'))})
    source_files['variable_type'] = \
    source_files['file_path'] \
        .replace(f'{input_filepath}/[0-9][a-z] - ', '', regex=True) \
            .replace('.xlsx', '', regex=True).replace('\W{1,}', '_', regex=True) \
                .str.lower()
    
    dfs = {}
    for i in source_files.index:
        dfs[source_files.loc[i,'variable_type']] =  pd.read_excel(source_files.loc[i,'file_path'])

    # load external population data from World Bank
    population = pd.read_csv(f'{external_filepath}/WPP2022_Demographic_Indicators_Medium.csv', usecols=['Location', 'Time', 'TPopulation1Jan', 'TPopulation1July'])
    population = population.rename(columns = {'Location' : 'country', 'Time' : 'year', 'TPopulation1Jan' : 'population_boy', 'TPopulation1July' : 'population_mid'})

    # aggregate country-level data for Yugoslavia and Netherlands Antilles 
    fmr_yugoslavia = \
    population \
        .loc[lambda x: x.country.isin(['Serbia', 'Croatia', 'Slovenia', 'Bosnia and Herzegovina', 'Macedonia'])] \
        .groupby('year') \
        .agg({'population_boy' : sum, 'population_mid' : sum}) \
        .reset_index() \
        .assign(country = 'Yugoslavia SFR') \
        .loc[:, ['country', 'year', 'population_boy', 'population_mid']]

    net_antilles = \
        population \
            .loc[lambda x: x.country.isin(['Curacao', 'Bonaire', 'Aruba', 'Sint Maarten (Dutch part)', 'Sint Eustatius', 'Saba', 'Netherlands Antilles'])] \
            .groupby('year') \
            .agg({'population_boy' : sum, 'population_mid' : sum}) \
            .reset_index() \
            .assign(country = 'Netherlands Antilles (former)') \
            .loc[:, ['country', 'year', 'population_boy', 'population_mid']]

    population = pd.concat([population, fmr_yugoslavia, net_antilles])

    # process producer data
    dfs['total_production'] = process_producer(dfs['total_production'], 'production', 'crop_year')
    dfs['domestic_consumption'] = process_producer(dfs['domestic_consumption'], 'consumption', 'crop_year')
    dfs['gross_opening_stocks'] = process_producer(dfs['gross_opening_stocks'], 'openstock', 'crop_year')
    dfs['exports_crop_year'] = process_producer(dfs['exports_crop_year'], 'exports', 'crop_year')
    dfs['exports_calendar_year'] = process_producer(dfs['exports_calendar_year'], 'exports', 'calendar_year')

    # merge producer data
    producer_cropyear = \
    dfs['gross_opening_stocks'] \
        .merge(dfs['total_production'], 
        how = 'left', 
        on = ['country', 'harvest_group','crop_year', 'crop_year_beg', 'crop_year_end'])

    producer_cropyear = \
        producer_cropyear \
            .merge(dfs['domestic_consumption'], 
            how = 'left', 
            on = ['country', 'harvest_group','crop_year', 'crop_year_beg', 'crop_year_end'])
            
    producer_cropyear = \
        producer_cropyear \
            .merge(dfs['exports_crop_year'], 
            how = 'left', 
            on = ['country', 'harvest_group','crop_year', 'crop_year_beg', 'crop_year_end'])

    # Calculate closing stock and stock adjustment between EOY (t-1) and BOY (t)
    producer_cropyear['closestock_1k_bags'] = close_stock_calc(producer_cropyear, '1k_bags')
    producer_cropyear['closestock_kg'] = close_stock_calc(producer_cropyear, 'kg')
    producer_cropyear['closestock_lb'] = close_stock_calc(producer_cropyear, 'lb')

    producer_cropyear.sort_values(['country', 'crop_year_beg'], inplace=True)
    producer_cropyear['stock_adj_1k_bags'] = stock_adj(producer_cropyear, '1k_bags')
    producer_cropyear['stock_adj_kg'] = stock_adj(producer_cropyear, 'kg')
    producer_cropyear['stock_adj_lb'] = stock_adj(producer_cropyear, 'lb')

    # Merge country population data
    countrymap = pd.DataFrame(
        {
        'country' : ['Democratic Republic of Congo', 'Tanzania', 'Trinidad & Tobago', 'Venezuela'],
        'country_pop' : ['Democratic Republic of the Congo', 'United Republic of Tanzania', 'Trinidad and Tobago', 'Venezuela (Bolivarian Republic of)']
        }
    )

    producer_cropyear = \
        producer_cropyear \
            .merge(countrymap, how = 'left', on='country', suffixes = ['', '_pop']) \
                .assign(country_pop = lambda x: np.where(x.country_pop.isna(), x.country, x.country_pop))

    producer_cropyear = \
        producer_cropyear \
            .merge(population, how = 'left', left_on=['country_pop', 'crop_year_beg'], right_on = ['country', 'year'], suffixes = ['', '_y']) \
                .drop(columns=['country_y', 'year']) \
                    .rename(columns={'population_boy' : 'population_beg'})

    producer_cropyear = \
        producer_cropyear \
            .merge(population, how = 'left', left_on=['country_pop', 'crop_year_end'], right_on = ['country', 'year'], suffixes = ['', '_y']) \
                .drop(columns=['country_y', 'year', 'population_mid_y']) \
                    .rename(columns={'population_boy' : 'population_end'})

    producer_cropyear.drop(columns='country_pop', inplace=True)

    # calendar year export data
    exports_calendar_year = \
    dfs['exports_calendar_year'] \
        .merge(countrymap, how = 'left', on='country', suffixes = ['', '_pop']) \
        .assign(country_pop = lambda x: np.where(x.country_pop.isna(), x.country, x.country_pop)) 

    exports_calendar_year = \
        exports_calendar_year \
            .merge(population, how = 'left', left_on=['country_pop', 'calendar_year'], right_on = ['country', 'year'], suffixes = ['', '_y']) \
                .drop(columns=['country_y', 'year'])
                
    exports_calendar_year.drop(columns='country_pop', inplace=True)

    # process importer data
    region_map = pd.DataFrame(
        {'country' : ['Austria','Belgium','Belgium/Luxembourg','Bulgaria','Croatia','Cyprus','Czechia','Denmark','Estonia','Finland','France','Germany',
        'Greece','Hungary','Ireland','Italy','Latvia','Lithuania','Luxembourg','Malta','Netherlands','Poland','Portugal',
        'Romania','Slovakia','Slovenia','Spain','Sweden','Japan','Norway','Russian Federation','Switzerland','Tunisia','United Kingdom','United States of America']
        ,
        'region' : ['Europe','Europe','Europe','Europe','Europe','Europe','Europe','Europe','Europe','Europe','Europe','Europe','Europe','Europe',
        'Europe','Europe','Europe','Europe','Europe','Europe','Europe','Europe','Europe','Europe','Europe','Europe','Europe',
        'Europe','Asia & Oceania','Europe','Europe','Europe','Africa','Europe','North America']
            }
        )
    
    dfs['imports'] = process_importer(dfs['imports'], 'imports', region_map)
    dfs['re_exports'] = process_importer(dfs['re_exports'], 're_exports', region_map)

    member = \
    dfs['imports'] \
    .merge(dfs['re_exports'], how = 'outer', on = ['region', 'country', 'ico_member', 'calendar_year'])

    # process non-member importer data
    dfs['non_member_imports'] = process_nonmember(dfs['non_member_imports'], 'imports')
    dfs['non_member_re_exports'] = process_nonmember(dfs['non_member_re_exports'], 're_exports')

    non_member = \
    dfs['non_member_imports'] \
    .merge(dfs['non_member_re_exports'], how = 'outer', on = ['region', 'country', 'ico_member', 'calendar_year'])

    # combine member/non-member import/re-export data
    imports_re_exports = pd.concat([member, non_member]).reset_index(drop=True)

    # add population data by country; country names are different for some countries.
    countrymap = \
        pd.DataFrame(
            {
                'country' : 
                    ['Abu Dhabi',
                    'China (Mainland)',
                    "Democratic People's Republic of Korea",
                    'Dubai',
                    'Hong Kong',
                    'Macao',
                    'Micronesia (Federated States of)',
                    'Netherlands Antilles (former)',
                    'Saint Vincent & the Grenadines',
                    'Taiwan',
                    'Turkey',
                    'USSR',
                    'Yugoslavia SFR'],

                'country_pop' :
                    ['United Arab Emirates',
                    'China',
                    "Dem. People's Republic of Korea",
                    'United Arab Emirates',
                    'China, Hong Kong SAR',
                    'China, Macao SAR',
                    'Micronesia',
                    'Netherlands Antilles (former)',
                    'Saint Vincent and the Grenadines',
                    'China, Taiwan Province of China',
                    'Türkiye',
                    'Russian Federation',
                    'Yugoslavia SFR']
            }
        )

    # map country names so that ICO data and UN population data are aligned
    imports_re_exports = \
    imports_re_exports \
        .merge(countrymap, how = 'left', on='country', suffixes=['', '_pop']) \
        .assign(country_pop = lambda x: np.where(x.country_pop.isna(), x.country, x.country_pop)) 

    # merge population data
    imports_re_exports = \
        imports_re_exports \
            .merge(population, how = 'left', left_on=['country_pop', 'calendar_year'], right_on = ['country', 'year'], suffixes = ['', '_y']) \
                .drop(columns=['year', 'country_y'])
                
    imports_re_exports.drop(columns='country_pop', inplace=True)

    # Write Interim Files
    for k in dfs.keys():
        dfs[k].to_csv(f'{interim_filepath}/{k}.csv', index=False)

    population.to_csv(f'{interim_filepath}/population_data.csv', index=False)
    
    # Write Output Files
    producer_cropyear.to_csv(f'{output_filepath}/producer_cropyear.csv', index=False)
    imports_re_exports.to_csv(f'{output_filepath}/imports_re_exports.csv', index=False)
    exports_calendar_year.to_csv(f'{output_filepath}/exports_calyear.csv', index=False)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
