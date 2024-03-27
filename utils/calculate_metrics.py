import os
import numpy as np
import pandas as pd

from functools       import partial
from multiprocessing import Pool

def get_country_investments(input_df, country=None, financial_sector=None):
    tmp_df = input_df[input_df['Country']==country]
    if 'real estate' in financial_sector or 'infrastructure' in financial_sector:
        return sum(tmp_df['Total country value (USD)'])
    else:
        return sum(tmp_df['Market Value(USD)'])

def get_country_investments_by_year(data_dir, country=None, verbose=False):
    if 'real_estate' in data_dir:      financial_sector = 'real estate'
    elif 'infrastructure' in data_dir: financial_sector = 'infrastructure'
    else:                              financial_sector = 'equity'
    investments = []
    for file in os.listdir(data_dir):
        if 'data_here' in file: continue
        year = int(file.split('_')[1])
        file_path = os.path.join(data_dir, file)
        if verbose: print(file_path, year, financial_sector)
        invested_amount = get_country_investments(input_df=pd.read_excel(file_path), 
                                                  country=country,
                                                  financial_sector=financial_sector
                                                  )
        investments.append((year, invested_amount))
    return investments


######### Using Multi processing
def process_file_investment(file_path, country=None):
    if 'real_estate' in file_path:      financial_sector = 'real estate'
    elif 'infrastructure' in file_path: financial_sector = 'infrastructure'
    else:                              financial_sector = 'equity'
    year = int(os.path.basename(file_path).split('_')[1])
    input_df = pd.read_excel(file_path)
    invested_amount = get_country_investments(input_df=input_df, country=country, financial_sector=financial_sector)
    return year, invested_amount
    
def get_country_investments_by_year_multiprocess(data_dir, country=None, verbose=False):
    # To get the investments made to a particular country in every year
    investments = []
    files = []
    for file in os.listdir(data_dir):
        if 'data_here' in file: continue
        file_path = os.path.join(data_dir, file)
        files.append(file_path)
        if verbose: print(file_path, year)
    with  Pool() as pool:
        # Use functools.partial to fix the country argument for process_file
        partial_process_file = partial(process_file_investment, country=country)
        results = pool.map(partial_process_file, files)
    investments.extend(results)
    return investments
##

def process_file_countries(file_path):
    year = int(os.path.basename(file_path).split('_')[1])
    input_df = pd.read_excel(file_path)
    return year, set(input_df['Country'])
    
def get_countries_by_year_multiprocess(data_dir, verbose=False):
    # To get the countries where investments have been  made
    countries = []
    files = []
    for file in os.listdir(data_dir):
        if 'data_here' in file: continue
        file_path = os.path.join(data_dir, file)
        files.append(file_path)
        if verbose: print(file_path, year)
    with  Pool() as pool:
        results = pool.map(process_file_countries, files)
    countries.extend(results)
    return countries

##
def process_file_total_investments(file_path):
    year = int(os.path.basename(file_path).split('_')[1])
    input_df = pd.read_excel(file_path)
    if 'real_estate' in file_path or 'infrastructure' in file_path:
        return year, sum(input_df['Total country value (USD)'])
    else:
        return year, sum(input_df['Market Value(USD)'])
    
def get_total_investments_by_year_multiprocess(data_dir, verbose=False):
    # To get the total investments every year
    total_investment = []
    files = []
    for file in os.listdir(data_dir):
        if 'data_here' in file: continue
        file_path = os.path.join(data_dir, file)
        files.append(file_path)
        if verbose: print(file_path, year)
    with  Pool() as pool:
        results = pool.map(process_file_total_investments, files)
    total_investment.extend(results)
    return total_investment
