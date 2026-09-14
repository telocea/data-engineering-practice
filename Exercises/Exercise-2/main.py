import requests
import pandas as pd
from bs4 import BeautifulSoup

def main():
    # your code here
    source = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
    # find file
    r = requests.get(source)
    if r.status_code == 200:
        # read file
        content = r.content
        soup = BeautifulSoup(content, 'html.parser')

        td_list = soup.find_all('td', string='2024-01-19 15:27')

        # load file into pandas
        all_highest = pd.DataFrame()
        for td in td_list:
            uri = source + td.find_previous_sibling('td').a.get('href')    
            df = pd.read_csv(uri).dropna(subset=['HourlyDryBulbTemperature'])
            df.loc[:, 'HourlyDryBulbTemperature'] = df['HourlyDryBulbTemperature'].apply(lambda x: extractNums(x))
            highest = df[df['HourlyDryBulbTemperature'] == df['HourlyDryBulbTemperature'].max()]
            all_highest = pd.concat([all_highest, highest], ignore_index=True)
        # find record(s) with highest 'HourlyDryBulbTemperature'
        max = all_highest[all_highest['HourlyDryBulbTemperature'] == all_highest['HourlyDryBulbTemperature'].max()]
        print(max)
    else:
        print('Invalid URI')

def extractNums(s):
    if isinstance(s, float) or isinstance(s, int):
        return s
    else:
        result = ''
        for i in s:
            if i.isnumeric() or i == '.':
                result += i
        if result:
            return float(result)
        else:
            return float('-inf')

if __name__ == "__main__":
    main()
