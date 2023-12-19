from resources import util
from multiprocessing.pool import ThreadPool as Pool
import pandas as pd
import os


def main():
    # Create a multithreading pool
    pool = Pool(24)
    config = util.get_config()

    print('\nStarted clean_data.py\n')

    # This loop run once for every stock and start a job for cleaning the data of that stock
    for quote in util.get_stock_list(config):
        pool.apply_async(clean_worker, (quote, config))
    
    pool.close()
    pool.join()
    
    print('\nCompleted clean_data.py\n')


def clean_worker(quote, config):
    # If files does not exist, skip stock
    if not os.path.isfile(f'data\\raw_data\\prices\\{quote}.csv'
    ) or not os.path.isfile(f'data\\raw_data\\splits\\{quote}.csv'
    ) or not os.path.isfile(f'data\\raw_data\\shares\\{quote}.txt'):
        return None
    
    try:
        print(f'Cleaning data for {quote}')

        # Import the prices/quote.csv dataframe
        main_df = pd.read_csv(f'data\\raw_data\\prices\\{quote}.csv')
        # Read number of shares from file
        shares_nb = int(util.read_txt(f'data\\raw_data\\shares\\{quote}.txt'))

        # Create 'Market Cap' column
        main_df.insert(main_df.shape[1], 'Market Cap', main_df['Open'] * shares_nb)
        # Create 'Delta' column
        main_df.insert(main_df.shape[1], 'Delta', main_df['Close'] - main_df['Open'])
        # Create 'DeltaPercent' column
        main_df.insert(main_df.shape[1], 'DeltaPercent', (main_df['Close'] - main_df['Open']) / main_df['Open'] * 100)

        # Save the dataframe as a CSV file
        main_df.to_csv(f'data\\clean_data\\{quote}.csv', encoding='utf-8', index=False)
        
        print(f'Cleaned data for {quote}')

    except Exception as e:
        print(f'\033[91mError while running clean_worker({quote}): {e}\033[0m')


if __name__ == '__main__':
    main()
