import json
import pandas as pd
import time


# This function reads the config.json file and returns it as a dictionary
def get_config():
    with open('config.json') as config_file:
        config = json.load(config_file)
    return config


# This function reads the stock_list.csv file and returns a list of stock quotes
def get_stock_list(config):
    df = pd.read_csv(f'data\\raw_data\\stock_list\\stock_list.csv')
    df = df.loc[df['Country'].isin(config["countries"])]

    return df["Symbol"].tolist()


# This function transforms a str into an int. Ex.: "5.2K" -> 5200
def value_to_int(value):
    x = value.upper()
    if 'QI' in x:
        return int(float(x.replace('QI', '')) * 10**18)
    elif 'QA' in x:
        return int(float(x.replace('QA', '')) * 10**15)
    elif 'T' in x:
        return int(float(x.replace('T', '')) * 10**12)
    elif 'B' in x:
        return int(float(x.replace('B', '')) * 10**9)
    elif 'M' in x:
        return int(float(x.replace('M', '')) * 10**6)
    elif 'K' in x:
        return int(float(x.replace('K', '')) * 10**3)
    else:
        return int(float(x))


# This function reads a file and returns it as a string
def read_txt(file_path):
    with open(file_path) as txt_file:
        text = txt_file.readline()
    return text


# This function makes a web resquest to the given url then returns the response (if the request fails, do it again until it works)
def force_request(url, session, config):
    while True:
        try:
            return session.get(url, headers=config["requeest_headers"])
        except Exception as _:
            print('\033[91mRequest failed\033[0m')
            time.sleep(3)


# This function writes a file to a given path
def write_file(path, content):
    with open(path.replace("/", "'"), 'w') as file:
        file.write(content)

