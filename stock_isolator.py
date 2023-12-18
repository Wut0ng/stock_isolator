from resources import util
import pandas as pd
import numpy as np
import datetime
import os
import time


def main():
    config = util.get_config()

    print('\nStarted stock_isolator.py\n')

    stock_isolator(config)
    
    print('\nCompleted stock_isolator.py\n')


def stock_isolator(config):
    # This function makes a list of stocks that satisfies every condiction for every day of the time period
    stock_list, df_list = get_stock_list(config)
    print(f'\n\033[94m{len(stock_list)} stocks meet the requirements\033[0m')
    if len(stock_list) < max(config["stock_isolator"]["keep"]["best"], config["stock_isolator"]["keep"]["worst"]):
        print('\033[91mNot enough stock for further analysis\033[0m')
        print('\033[91mTry with less strict requirements\033[0m')
        return None
    print('\n')

    # This function computes the N best and worst stocks of every day of the time period
    best_stocks = get_best_stocks(config, stock_list, df_list)
    # This function saves the best_stocks to a file
    save_best_stocks(config, best_stocks)
    # This function display the best_stocks to the user
    display_best_stocks(config, stock_list, best_stocks)


# This function makes a list of stocks that satisfies every condiction for every day of the time period
def get_stock_list(config):
    stock_list = []
    df_list = []
    # This loop run once for every stock
    for quote in util.get_stock_list(config):

        # Check if stock has clean data
        if not os.path.isfile(f'data\\clean_data\\{quote}.csv'):
            continue
        
        # Check if a certain stock meets the conditions
        df_result = stock_meets_conditions(quote, config)
        if df_result is not None:
            # If the stock meet the requirements, then put it in the list
            # print(f'Stock  {quote:<5} does meet the requirements \033[92m✓\033[0m')
            stock_list.append(quote)
            df_list.append(df_result)
        # else:
            # print(f"Stock  {quote:<5} does not meet the requirements \033[91m✗\033[0m")

    return stock_list, df_list

# This function checks if a certain stock meets the conditions
def stock_meets_conditions(quote, config):
    df = pd.read_csv(f'data\\clean_data\\{quote}.csv')
    df['Date'] = pd.to_datetime(df['Date'])

    period_start = datetime.datetime(year=config["stock_isolator"]["period"]["start"]["year"], month=config["stock_isolator"]["period"]["start"]["month"], day=config["stock_isolator"]["period"]["start"]["day"])
    period_end = datetime.datetime(year=config["stock_isolator"]["period"]["end"]["year"], month=config["stock_isolator"]["period"]["end"]["month"], day=config["stock_isolator"]["period"]["end"]["day"])
    
    least_recent_value = df.at[df.index[1], 'Date']
    most_recent_value = df.at[df.index[-1], 'Date']

    # Check if data is present for the requested time period
    if least_recent_value > period_start or most_recent_value < period_end:
        return None

    # Keep only part of the DF that is in the requested time period
    df = df[df['Date'] >= period_start]
    df = df[df['Date'] <= period_end]
    df.index = df['Date']
    df.index.name = None

    # This loop run once for every condition
    for condition in config["stock_isolator"]["conditions"]:
        # Check if stock meet MIN conditions
        if "min_value" in condition and (df[condition["condition"]] < util.value_to_int(condition["min_value"])).any():
            return None
        # Check if stock meet MAX conditions
        if "max_value" in condition and (df[condition["condition"]] > util.value_to_int(condition["max_value"])).any():
            return None

    return df


# This function computes the N best and worst stocks of every day of the time period
def get_best_stocks(config, stock_list, df_list):
    # Number of best and worst stocks to highlight
    keep_best_nb = config["stock_isolator"]["keep"]["best"]
    keep_worst_nb = config["stock_isolator"]["keep"]["worst"]
    
    # Next 5 lines create a very big dataframes where rows are days within the time period and columns are stocks within the list of stocks 
    deltapercent_col_list = [ df["DeltaPercent"] for df in df_list ]
    data_df = pd.concat(deltapercent_col_list, axis=1)
    data_df.columns = stock_list
    data_df.fillna(0.0, inplace=True)
    data_arrays = data_df.to_numpy()

    # Calculate the best and worst stocks for very day using numpy.argsort()
    best_indexes = data_arrays.argsort()[:,::-1][:,:keep_best_nb]
    best_values = data_arrays[np.arange(data_arrays.shape[0])[:,None],best_indexes]
    worst_indexes = data_arrays.argsort()[:,:keep_worst_nb]
    worst_values = data_arrays[np.arange(data_arrays.shape[0])[:,None],worst_indexes]

    # The following 11 lines transform the result into a single readable dataframe
    best_values_df = pd.DataFrame(data=best_values, index=data_df.index, columns=[f'{x+1}BN' for x in range(keep_best_nb)])
    best_indexes_df = pd.DataFrame(data=best_indexes, index=data_df.index, columns=[f'{x+1}BQ' for x in range(keep_best_nb)])
    best_indexes_df.replace({i: name for i, name in enumerate(data_df.columns.to_list())}, inplace=True)
    best_df = pd.concat([best_values_df, best_indexes_df], axis=1)
    best_df.sort_index(axis=1, inplace=True)
    worst_values_df = pd.DataFrame(data=worst_values, index=data_df.index, columns=[f'{x+1}WN' for x in range(keep_worst_nb)])
    worst_indexes_df = pd.DataFrame(data=worst_indexes, index=data_df.index, columns=[f'{x+1}WQ' for x in range(keep_worst_nb)])
    worst_indexes_df.replace({i: name for i, name in enumerate(data_df.columns.to_list())}, inplace=True)
    worst_df = pd.concat([worst_values_df, worst_indexes_df], axis=1)
    worst_df.sort_index(axis=1, inplace=True)
    final_df = pd.concat([best_df, worst_df], axis=1)

    return final_df


# This function saves the best_stocks to a file
def save_best_stocks(config, best_stocks):
    best_stocks.to_csv(f'logs\\stock_isolator_{time.time()}.csv', encoding='utf-8')


# This function display the best_stocks to the user
def display_best_stocks(config, stock_list, best_stocks):
    print("\033[92mList of stocks that meet the requirements ✓ :\033[0m\n")
    x = 0
    for stock in stock_list:
        if x >= 18:
            print("")
            x = 0
        x += 1
        print(f'{stock:<5}', end=' ')

    print("\n\n\033[92mBest and worst stocks for every day:\033[0m\n")
    with pd.option_context('display.max_rows', None, 'display.float_format', '{:+,.2f}%'.format):
        print(best_stocks)


if __name__ == '__main__':
    main()
