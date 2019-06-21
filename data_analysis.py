import pandas as pd
import csv

full_transportation_keywords = ['road', 'roadway', 'street', 'bridge', 'drive', 'avenue', 'bus line reopen/open', 'megabus reopen/open', 'metro', 'subway', 'sub', 'trains', 'train', 'transit']
full_utilities_keywords = ['power', 'water', 'gas', 'electricity', 'emergency power', 'emergency generator', 'black out', 'blackout', 'blackoutnyc', 'con ed', 'con edison', 'coned', 'dark', 'darker', 'downed electrical wires', 'POWER down', 'POWER not expected', 'POWER off', 'POWER out', 'POWER outage', 'goodbye POWER', 'knock out POWER', 'lose POWER', 'losing POWER', 'lost POWER', 'njpower', 'no POWER', 'noPOWER', 'off the grid', 'powerless', 'shut off POWER', 'taken POWER', 'transformer exploding', 'transformer explosion', 'w/o POWER', 'wait POWER return', 'without POWER', 'candle']
full_early_recovery_keywords = ['shelter', 'snuggled up safely inside', 'stay home', 'stay inside', ' stay safe', 'staysafe', 'evacuate', 'evacuated', 'evacuating', 'evacuation', 'evacuee', 'head away from', 'leave home', 'leaving city', 'police ask leave', 'seeking refuge', 'sleep outside', 'stay with friends', 'hotel', 'housing', 'shelter', 'ambulance', 'emergency response', 'escape', 'escaped', 'escaping', 'first aid', 'rescue', 'rescued', 'rescuing']
full_food_keywords = ['feed victims', 'food trucks', 'free lunch', 'free meals', 'get meals', 'refugee meal', 'nutri', 'nutrition']

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
  lowered_message = ''
  if type(message) is str:
    lowered_message = message.lower()
  else:
    return data

  for keyword in keywords:
    lowered_keyword = keyword.lower()
    if (lowered_message.find(' ' + lowered_keyword + ' ') != -1) or (lowered_message.find(lowered_keyword + ' ') != -1) or (lowered_message.find(' ' + lowered_keyword) != -1):
      row['keyword_category'] = keyword_category
      data = data.append(row)
  return data

def keyword_count_by_location_grouped_by_hour(data, writeCSV):
  data['keyword_category'] = ''
  new_data = pd.DataFrame({'time':[], 'location': [], 'account': [], 'message': [], 'keyword_category': []})
  for i, row in data.iterrows():
    if type(row['message']) is not str or row['message'].find('re:') == 0:
      continue
    new_data = findKeywordsInMessageAndAppendToData(new_data, full_transportation_keywords, row['message'], row, 'transportation')
    new_data = findKeywordsInMessageAndAppendToData(new_data, full_utilities_keywords, row['message'], row, 'utilities')
    new_data = findKeywordsInMessageAndAppendToData(new_data, full_early_recovery_keywords, row['message'], row, 'early_recovery')
    new_data = findKeywordsInMessageAndAppendToData(new_data, full_food_keywords, row['message'], row, 'food')
    if i % 500 == 0:
      print('row: ' + str(i))

  print(new_data)
  new_data.index = pd.to_datetime(new_data['time'])
  grouped = new_data.groupby([pd.Grouper(freq='5Min'), 'location', 'keyword_category'])['keyword_category'].count()
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
  data = pd.read_csv('./data/MC3/Yint.csv')
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