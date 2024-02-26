import requests
import redis
import json
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

class APIStuff:
    def __init__(self, url):
        self.url = url
        
    def get_data(self, headers, params):
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    
class RedisStuff:
    def __init__(self, host, port, password):
        self.r = redis.StrictRedis(host=host, port=port, password=password)
        
    def set_json(self, key, data):
        json_data = json.dumps(data)
        self.r.set(key, json_data)

    def get_json(self, key):
        json_data = self.r.get(key)
        if json_data is not None:
            return json.loads(json_data)
        else:
            return None

    def get_keys(self):
        return self.r.keys()
    
if __name__ == "__main__":
    
    url = "https://covid-19-statistics.p.rapidapi.com/reports/total"
    headers = {"X-RapidAPI-Key": "239f222809msha2e4818a47c0fdfp19a3e4jsna22b27732628", "X-RapidAPI-Host": "covid-19-statistics.p.rapidapi.com"}
    querylist = [{'2020','2021','2022','2023'}, {'-01','-02', '03', '-04', '05', '-06','-12'}]
    data_dict = {}
    api_client = APIStuff(url)
    if len(data_dict) == 0:
        for year in range(2020,2024):
            for month in range(1,12):
                tempmonth = f'{month:02}'
                params = {'date' : str(year) + '-' + tempmonth + '-01'}
                data_dict[str(year) + '-' + tempmonth + '-01'] = api_client.get_data(headers,params)
    
    host = 'redis-11079.c325.us-east-1-4.ec2.cloud.redislabs.com'
    port = 11079
    password = 'tQoaqjXufBcVVtPLd26L7IrMGtQXYsTn'
    redis_dict = {}
    redis_client = RedisStuff(host, port, password)
    for thing in data_dict:
        data = data_dict[thing]['data']
        if len(data) != 0:
            print(thing)
            redis_client.set_json(thing, data)
 
    key_list = redis_client.get_keys()
    for key in key_list:
        print(key)
        redis_dict[key] = redis_client.get_json(key)

keys = list(redis_dict.keys())
deaths_values = [redis_dict[key].get('deaths', None) for key in keys]
fatality_rates = [redis_dict[key].get('fatality_rate', None) for key in keys]
dates_values = [redis_dict[key].get('date', None) for key in keys]
dates_values = pd.to_datetime(dates_values)

df = pd.DataFrame({'date': dates_values, 'deaths': deaths_values, 'fatality_rate': fatality_rates})
df = df.sort_values(by='date')
df = df.reset_index(drop=True)
df['date'] = pd.to_datetime(df['date'])
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
percentiles = np.percentile(df['fatality_rate'], [0, 1, 24, 76, 99, 100])
df['fatality_rate_bucket'] = pd.cut(df['fatality_rate'], bins=percentiles, labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'], include_lowest=True)


plt.figure(dpi=100, figsize=(12, 6))
plt.plot(df['date'], df['deaths'], marker='o')
plt.xlabel('Date')
plt.ylabel('Deaths')
plt.title('Covid Deaths Over Time')
plt.ylim(0, max(deaths_values) + 100000)
plt.ticklabel_format(axis='y', style='plain')
plt.tight_layout()
plt.grid(True)
plt.show()



print('Done!')