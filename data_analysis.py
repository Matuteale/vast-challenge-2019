import pandas as pd
import csv
from fuzzywuzzy import fuzz
from collections import OrderedDict

sewer_and_water = ['discharged', 'discharge', 'drain', 'drainage', 'flood', 'hygiene', 'irrigation', 'pipes', 'pump', 'river', 'sanitary', 'sewage', 'sewer', 'stream', 'underground', 'wash', 'waste', 'water']
power = ['valve', 'heat', 'gas', 'power', 'electric', 'candle', 'flashlight', 'generator', 'black out', 'blackout', 'dark', 'radiation', 'radio rays', 'energy', 'nuclear', 'fuel', 'battery', 'radiant']
roads_and_bridges = ['airport', 'avenue', 'bridge', 'bus', 'congestion', 'drive', 'flight', 'jam', 'logistic', 'metro', 'mta', 'road', 'street', 'subway', 'traffic', 'train', 'transit', 'transportation', 'highway', 'route', 'lane']
medical = ['medical', 'red cross', 'food', 'emergency', 'urgent', 'evacuate', 'evacuating', 'evacuation', 'protection', 'ambulance', 'escape', 'first aid', 'rescue', 'rescuing', 'dead', 'death', 'kill', 'help', 'help out', 'help with', 'volunteer', 'volunteering', 'explosion', 'exploding', 'explode', 'victim', 'fatalities']
buildings = ['collapse', 'housing', 'house']
full_retweets_keywords = ['re:']

def writeCSVFromData(data, path, header, omitIndex):
  outcsv = open(path, 'w')
  outcsv.truncate()
  writer = csv.writer(outcsv)
  writer.writerow(header)
  outcsv.close()
  if omitIndex:
    data.to_csv(path, mode = 'a', header = False, index_label=False, index=False)
  else:
    data.to_csv(path, mode = 'a', header = False)

def count_user_tweets(data):
  grouped = data.groupby('account')
  print(grouped.size())

def count_user_retweets(data):
  retweets = data[data['message'].str.startswith('re:', na = False)]
  grouped = retweets.groupby('account')
  print(grouped.size())

def users_that_tweet_at_diff_locations(data):
  grouped_unique_locations = data.groupby('account')['location'].nunique().sort_values()
  filtered = grouped_unique_locations[grouped_unique_locations > 1]
  print(filtered)

def user_count_by_location_grouped_by_hour(data, writeCSV):
  times = pd.DatetimeIndex(data.time)
  grouped = data.groupby([times.month, times.day, times.hour, 'location'])['account'].nunique()
  print(grouped)
  if writeCSV:
    writeCSVFromData(grouped, './output/user_count_by_location_grouped_by_hour.csv', ['month', 'day', 'hour', 'location', 'unique_accounts_count'], False)

def findKeywordsInMessageAndAppendToData(data, keywords, message, row, keyword_category):
  if type(message) is not str:
    return data

  for keyword in keywords:
    if fuzz.partial_ratio(message.lower(), keyword.lower()) >= 60:
      row['keyword_category'] = keyword_category
      data = data.append(row)
  return data

def keyword_count_by_location_grouped_by_hour_retweets_only(data, writeCSV):
  data = data.loc[data['message'].str.find('re:') == 0]
  data['keyword_category'] = ''
  new_data = pd.DataFrame({'time':[], 'location': [], 'account': [], 'message': [], 'keyword_category': []})
  for i, row in data.iterrows():
    new_data = findKeywordsInMessageAndAppendToData(new_data, full_retweets_keywords, row['message'], row, 'retweets')
    if i % 500 == 0:
      print('row: ' + str(i))

  print(new_data)
  new_data.index = pd.to_datetime(new_data['time'])
  grouped = new_data.groupby([pd.Grouper(freq='10Min'), 'location', 'keyword_category'])['keyword_category'].count()
  print(grouped)
  if writeCSV:
    writeCSVFromData(grouped, './output/keyword_count_by_location_grouped_by_hour_retweets_only.csv', ['time', 'location', 'keyword_category', 'count'], False)

def keyword_count_by_location_grouped_by_hour(data, writeCSV):
  data = data.loc[data['message'].str.find('re:') != 0]
  data['keyword_category'] = ''
  new_data = pd.DataFrame({'time':[], 'location': [], 'account': [], 'message': [], 'keyword_category': []})
  for i, row in data.iterrows():
    new_data = findKeywordsInMessageAndAppendToData(new_data, roads_and_bridges, row['message'], row, 'road')
    new_data = findKeywordsInMessageAndAppendToData(new_data, power, row['message'], row, 'power')
    new_data = findKeywordsInMessageAndAppendToData(new_data, medical, row['message'], row, 'rescue')
    new_data = findKeywordsInMessageAndAppendToData(new_data, sewer_and_water, row['message'], row, 'sewer')
    if i % 500 == 0:
      print('row: ' + str(i))

  print(new_data)
  new_data.index = pd.to_datetime(new_data['time'])
  grouped = new_data.groupby([pd.Grouper(freq='10Min'), 'location', 'keyword_category'])['keyword_category'].count()
  print(grouped)

  if writeCSV:
    writeCSVFromData(grouped, './output/keyword_count_by_location_grouped_by_hour.csv', ['time', 'location', 'keyword_category', 'count'], False)

def influencers_info(data, writeCSV, topNumber):
  data_without_retweets = data.loc[data['message'].str.find('re:') != 0]
  grouped_by_account = data_without_retweets.groupby('account')['message'].count()
  top_account_tweets = grouped_by_account.nlargest(topNumber)
  finalDataFrame = pd.DataFrame({'account':[], 'tweets': [], 'mentions': [], 'times_retweeted': []})
  data_retweets_only = data.loc[data['message'].str.find('re:') == 0]
  for account, tweet_count in top_account_tweets.iteritems():
    mentions = data.loc[(data['account'] != account) & (data['message'].str.find('@' + account) != -1)]
    tweets = data_without_retweets.loc[data_without_retweets['account'] == account]
    tweets['message'] = 're: ' + tweets['message'].astype(str)
    retweets = data_retweets_only.loc[(data_retweets_only['account'] != account) & (data_retweets_only['message'].isin(tweets['message']))]
    row = pd.DataFrame({'account':[account], 'tweets': [tweet_count], 'mentions': [mentions.size], 'times_retweeted': [retweets.size]})
    finalDataFrame = finalDataFrame.append(row)
  finalDataFrame = finalDataFrame[['account', 'tweets', 'mentions', 'times_retweeted']]
  print(finalDataFrame)
  if writeCSV:
    writeCSVFromData(finalDataFrame, './output/influencers_info.csv', ['account', 'tweets', 'mentions', 'times_retweeted'], True)

def main():
  print('\n\nReading csv data...\n\n')
  data = pd.read_csv('./data/MC3/Yint.csv')
  data.info()
  accounts_to_filter = ['______3333_____', 'Opportunities2', 'Opportunities1', 'Syndicated5', 'CantonCoordon2', 'Syndicated4', 'Syndicated348', 'JordanWantsBac0n', 'J0rdanWantsBacon', 'JordanWantsBacon', 'handle']
  data = data.loc[~data['account'].isin(accounts_to_filter)]

  # keyword_count_by_location_grouped_by_hour(data, True)
  influencers_info(data, True, 10)
  # print('\n\nCounting tweets by user...\n\n')
  # count_user_tweets(data)
  # print('\n\nCounting retweets by user...\n\n')
  # count_user_retweets(data)
  # print('\n\nCalculating users that tweet at different locations...\n\n')
  # users_that_tweet_at_diff_locations(data)



if __name__== '__main__':
  main()