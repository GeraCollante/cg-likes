import pandas as pd
import requests
import dropbox
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime
import re
from datetime import date
from os.path import join

DATADIR = 'data'


def get_word_parenthesis(s: str) -> str:
    return s[s.find("(") + 1:s.find(")")]


def get_features(url: str):
    page = requests.get(url)
    my_soup = BeautifulSoup(page.content, 'html.parser')
    likes = get_likes(my_soup)
    symbol = get_symbol(my_soup)
    price = get_price(my_soup)
    market_cap = get_market_cap(my_soup)
    volume_cap = get_volume_cap(my_soup)
    return symbol, price, likes, market_cap, volume_cap
    # print(f'likes: {likes}, symbol: {symbol}, price: {price}, market_cap: {market_cap}, volume_cap: {volume_cap}')
    # return my_soup


def get_price(my_soup: BeautifulSoup) -> float:
    try:
        values = my_soup.find_all(class_="no-wrap")
        price = float(values[0].text.replace(',', '.').replace('$', ''))
    except:
        price = -1
    return price


def get_market_cap(my_soup: BeautifulSoup) -> int:
    try:
        values = my_soup.find_all(class_="no-wrap")
        market_cap = int(values[1].text.replace('.', '').replace('$', ''))
    except:
        market_cap = -1
    return market_cap


def get_volume_cap(my_soup: BeautifulSoup) -> float:
    try:
        values = my_soup.find_all(class_="no-wrap")
        volume_cap = int(values[2].text.replace('.', '').replace('$', ''))
    except:
        volume_cap = -1
    return volume_cap


def get_likes(my_soup: BeautifulSoup) -> int:
    try:
        # page = requests.get(url)
        # my_soup = BeautifulSoup(page.content, 'html.parser')
        txt = my_soup.find_all('span', 'ml-1')[-1].text
        rgx = re.search("\d*\.\d*", txt)
        likes = int(rgx.group(0).replace('.', ''))
    except:
        likes = -1
    return likes


def get_symbol(my_soup: BeautifulSoup) -> str:
    try:
        txt = my_soup.find(class_="mr-md-3 mx-2 mb-md-0 text-3xl font-semibold").text
        symbol = get_word_parenthesis(txt)
    except:
        symbol = "UNK"
    return symbol


def get_links(url: str) -> pd.DataFrame:
    # Get Soup
    response = requests.get(URL)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Get all links of table
    table = soup.find('table')

    links = []
    for tr in table.findAll("tr"):
        trs = tr.findAll("td")
        for each in trs:
            try:
                link = each.find('a')['href']
                links.append(link)
            except:
                pass

    # Get DataFrame with links
    df = pd.DataFrame(['https://www.coingecko.com' + link for link in links]).rename(columns={0: 'link'})

    # Get only useful links
    df = df.iloc[::3]

    # Reset index
    df.reset_index(drop=True)

    return df


class TransferData:
    def __init__(self, access_token):
        self.access_token = access_token

    def upload_file(self, file_from, file_to):
        """upload a file to Dropbox using API v2
        """
        dbx = dropbox.Dropbox(self.access_token)

        with open(file_from, 'rb') as f:
            dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)


TOKEN = 'sl.Aw9nbONUNSDkePBnbJ861vl9xk4AoULLVOhyd6JDEFOBggwvLCO9CPJQaj6fEoQZLXrHefYaBTwcLdMYeIQB8F3iYWVsh7IWHErg3DSKuBpmh13w8JtnJu2jCm85xPSIeu6nH4s '

if __name__ == "__main__":
    today_date = date.today().strftime("%y-%m-%d-") + '.csv'
    FILENAME = join(DATADIR, today_date)

    transferData = TransferData(TOKEN)

    tqdm.pandas()

    URL = 'https://www.coingecko.com/es/monedas/all?utf8=%E2%9C%93&filter_market_cap=&filter_24h_volume=&filter_price' \
          '=&filter_24h_change=&filter_category=&filter_market=Binance&filter_asset_platform=&filter_hashing_algorithm' \
          '=&sort_by=change30d&commit=Search'

    # Get links
    df = get_links(URL)
    df.drop_duplicates(inplace=True)

    try:
        df_crypto = pd.read_csv(FILENAME)
    except:
        df_crypto = pd.DataFrame()
        pass

    # Get features
    N_SAMPLES = 4

    df_temp = pd.DataFrame(df[:N_SAMPLES].link.progress_apply(lambda x: get_features(x)).to_list(),
                           columns=['symbol',
                                    'price',
                                    'likes',
                                    'market_cap',
                                    'volume_cap'])

    df_temp['date'] = datetime.now()
    df_temp.drop_duplicates(inplace=True)

    df_crypto = pd.concat([df_crypto, df_temp])

    df_crypto.to_csv(FILENAME, index=False)

    # API v2
    transferData.upload_file(FILENAME, '/' + FILENAME)