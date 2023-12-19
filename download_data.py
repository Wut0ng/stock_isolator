from resources import util
from multiprocessing.pool import ThreadPool as Pool
import re
import requests
import os
import time


def main():
    # Create a multithreading pool
    pool = Pool(1)
    config = util.get_config()

    print('\nStarted download_data.py\n')

    with requests.Session() as session:
        # This loop run once for every stock and start a job for downloading the data of that stock
        for quote in util.get_stock_list(config):
            pool.apply_async(shares_worker, (quote, session, config))
            pool.apply_async(split_worker, (quote, session, config))
            pool.apply_async(price_worker, (quote, session, config))
        
        pool.close()
        pool.join()
    
    print('\nCompleted download_data.py\n')


def shares_worker(quote, session, config):
    # If the file already exists, no need to download it
    if os.path.isfile(f'data\\raw_data\\shares\\{quote}.txt'):
        return None

    try:
        # Get statistics of this stock from YahooFinance
        response = util.force_request(f'https://finance.yahoo.com/quote/{quote}/key-statistics', session, config)
        
        # If request failed, throw error
        if response.status_code != 200 or 'Invalid cookie' in response:
            raise PermissionError(f'Request failed with code {response.status_code}')
        
        # Isolate the nb of shares info using a REGEX
        nb_shares_str_list = re.split(r'(?:<span>Shares Outstanding</span> <sup aria-label="Shares outstanding is taken from the most recently filed quarterly or annual report and Market Cap is calculated using shares outstanding.">5</sup></td><td class="Fw\(500\) Ta\(end\) Pstart\(10px\) Miw\(60px\)")(.*?)(?:</td>)', response.text)
        
        # Stop process if YahooFinance does not list stock (the REGEX did not find nb of shares)
        if len(nb_shares_str_list) < 2:
            return None
        
        nb_shares_str = nb_shares_str_list[1]
        
        # Stop process if number of shares is N/A
        if nb_shares_str == "><span>N/A</span>":
            return None
        
        # Transform nb of shares str to int
        nb_shares_int = util.value_to_int(nb_shares_str.replace('>', ''))
        
        # Save the info in a TXT file
        util.write_file(f'data\\raw_data\\shares\\{quote}.txt', str(nb_shares_int))

        print(f'Downloaded SHARES data for {quote}')

    except Exception as e:
        print(f'\033[91mError while running shares_worker({quote}): {e}\033[0m')
        print(f'https://finance.yahoo.com/quote/{quote}/key-statistics')


def split_worker(quote, session, config):
    # If the file already exists, no need to download it
    if os.path.isfile(f'data\\raw_data\\splits\\{quote}.csv'):
        return None
    
    try:
        # Get stock split data from YahooFinance
        response = util.force_request(
            f'https://query1.finance.yahoo.com/v7/finance/download/{quote}?period1={config["period_start"]}&period2={config["period_end"]}&interval=1d&events=split',
        session, config)

        # If request failed, throw error
        if response.status_code != 200 or 'Invalid cookie' in response:
            raise PermissionError(f'Request failed with code {response.status_code}')

        # Save data in a CSV file
        util.write_file(f'data\\raw_data\\splits\\{quote}.csv', response.text)

        print(f'Downloaded SPLIT  data for {quote}')

    except Exception as e:
        print(f'\033[91mError while running split_worker({quote}): {e}\033[0m')
        print(f'https://query1.finance.yahoo.com/v7/finance/download/{quote}?period1={config["period_start"]}&period2={config["period_end"]}&interval=1d&events=split')


def price_worker(quote, session, config):
    # If the file already exists, no need to download it
    if os.path.isfile(f'data\\raw_data\\prices\\{quote}.csv'):
        return None
    
    try:
        # Get stock price data from YahooFinance
        response = util.force_request(
            f'https://query1.finance.yahoo.com/v7/finance/download/{quote}?period1={config["period_start"]}&period2={config["period_end"]}&interval=1d&events=history',
        session, config)

        # If request failed, throw error
        if response.status_code != 200 or 'Invalid cookie' in response:
            raise PermissionError(f'Request failed with code {response.status_code}')

        # Save data in a CSV file
        util.write_file(f'data\\raw_data\\prices\\{quote}.csv', response.text)

        print(f'Downloaded PRICE  data for {quote}')

    except Exception as e:
        print(f'\033[91mError while running price_worker({quote}): {e}\033[0m')
        print(f'https://query1.finance.yahoo.com/v7/finance/download/{quote}?period1={config["period_start"]}&period2={config["period_end"]}&interval=1d&events=history')


if __name__ == '__main__':
    main()

