import pandas as pd
import csv
from fuzzywuzzy import fuzz

sewer_and_water = ["discharged", "discharge", "drain", "drainage", "flood", "hygiene", "irrigation", "pipes", "pump", "river", "sanitary", "sewage", "sewer", "stream", "underground", "wash", "waste", "water"]
power = ["valve", "heat", "gas", "power", "electric", "candle", "flashlight", "generator", "black out", "blackout", "dark", "radiation", "radio rays", "energy", "nuclear", "fuel", "battery", "radiant"]
roads_and_bridges = ["airport", "avenue", "bridge", "bus", "congestion", "drive", "flight", "jam", "logistic", "metro", "mta", "road", "street", "subway", "traffic", "train", "transit", "transportation", "highway", "route", "lane"]
medical = ["medical", "red cross", "food", "emergency", "urgent", "evacuate", "evacuating", "evacuation", "protection", "ambulance", "escape", "first aid", "rescue", "rescuing", "dead", "death", "kill", "help", "help out", "help with", "volunteer", "volunteering", "explosion", "exploding", "explode", "victim", "fatalities"]
buildings = ["collapse", "housing", "house"]
full_retweets_keywords = ['re:']

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

def findKeywordsInMessageAndAppendToData(data, keywords, message, row, keyword_category):
  if type(message) is not str:
    return data

  for keyword in keywords:
    if fuzz.partial_ratio(message.lower(), keyword.lower()) >= 60:
      row['keyword_category'] = keyword_category
      data = data.append(row)
  return data

def keyword_count_by_location_grouped_by_hour_retweets_only(data, writeCSV):
  data['keyword_category'] = ''
  new_data = pd.DataFrame({'time':[], 'location': [], 'account': [], 'message': [], 'keyword_category': []})
  for i, row in data.iterrows():
    if type(row['message']) is not str or row['message'].find('re:') == 0:
      new_data = findKeywordsInMessageAndAppendToData(new_data, full_retweets_keywords, row['message'], row, 'retweets')
      continue
    if i % 500 == 0:
      print('row: ' + str(i))

  print(new_data)
  new_data.index = pd.to_datetime(new_data['time'])
  grouped = new_data.groupby([pd.Grouper(freq='10Min'), 'location', 'keyword_category'])['keyword_category'].count()
  print(grouped)

  if writeCSV:
    path = './output/keyword_count_by_location_grouped_by_hour_retweets_only.csv'
    outcsv = open(path, 'w')
    outcsv.truncate()
    writer = csv.writer(outcsv)
    writer.writerow(['time', 'location', 'keyword_category', 'count'])
    outcsv.close()
    grouped.to_csv(path, mode = 'a', header = False)

def keyword_count_by_location_grouped_by_hour(data, writeCSV):
  data['keyword_category'] = ''
  new_data = pd.DataFrame({'time':[], 'location': [], 'account': [], 'message': [], 'keyword_category': []})
  for i, row in data.iterrows():
    if type(row['message']) is not str or row['message'].find('re:') == 0:
      continue
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
    path = './output/keyword_count_by_location_grouped_by_hour.csv'
    outcsv = open(path, 'w')
    outcsv.truncate()
    writer = csv.writer(outcsv)
    writer.writerow(['time', 'location', 'keyword_category', 'count'])
    outcsv.close()
    grouped.to_csv(path, mode = 'a', header = False)

def user_count_by_location_grouped_by_hour(data, writeCSV):
  times = pd.DatetimeIndex(data.time)
  grouped = data.groupby([times.month, times.day, times.hour, 'location'])['account'].nunique()
  print(grouped)
  if writeCSV:
    path = './output/user_count_by_location_grouped_by_hour.csv'
    outcsv = open(path, 'w')
    outcsv.truncate()
    writer = csv.writer(outcsv)
    writer.writerow(['month', 'day', 'hour', 'location', 'unique_accounts_count'])
    outcsv.close()
    grouped.to_csv(path, mode = 'a', header = False)

def main():
  print('\n\nReading csv data...\n\n')
  data = pd.read_csv('./data/MC3/Yint-no-retweets.csv')
  data.info()
  keyword_count_by_location_grouped_by_hour(data, True)
  # print('\n\nCounting tweets by user...\n\n')
  # count_user_tweets(data)
  # print('\n\nCounting retweets by user...\n\n')
  # count_user_retweets(data)
  # print('\n\nCalculating users that tweet at different locations...\n\n')
  # users_that_tweet_at_diff_locations(data)



if __name__== '__main__':
  main()