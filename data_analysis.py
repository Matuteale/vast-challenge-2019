import pandas as pd
import csv

full_utilities_keywords = ['Sewer/water', 'Bridge', 'Gas', 'Power', 'Injured', 'Hurt', 'Smoke', 'No power', 'No water', 'No gas', 'Water\'s out', 'Fatalities', 'Highway', 'Road', 'pit']
full_problems_keywords = ['Problem', 'Issue', 'Damage', 'Accident', 'Closed', 'Destroyed', 'Broken', 'Out of', 'Urgent', 'Help', 'Vibrate', 'Disaster']

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

def findKeywordsInMessageAndAppendToData(data, keywords, message, row):
  lowered_message = ''
  if type(message) is str:
    lowered_message = message.lower()
  else:
    return data

  matched = False
  for keyword in keywords:
    lowered_keyword = keyword.lower()
    if (lowered_message.find(' ' + lowered_keyword + ' ') != -1) or (lowered_message.find(lowered_keyword + ' ') != -1) or (lowered_message.find(' ' + lowered_keyword) != -1):
      row['keyword'] = lowered_keyword
      data = data.append(row)
      matched = True

  if not matched:
    data = data.append(row)
  return data

def keyword_count_by_location_grouped_by_hour(data, writeCSV):
  utilities_keywords = full_utilities_keywords
  problems_keywords = full_problems_keywords
  data['keyword'] = ''
  new_data = pd.DataFrame({'time':[], 'location': [], 'account': [], 'message': [], 'keyword': []})
  for i, row in data.iterrows():
    new_data = findKeywordsInMessageAndAppendToData(new_data, utilities_keywords, row['message'], row)
    new_data = findKeywordsInMessageAndAppendToData(new_data, problems_keywords, row['message'], row)

  print(new_data)
  times = pd.DatetimeIndex(new_data.time)
  grouped = new_data.groupby([times.month, times.day, times.hour, 'location', 'keyword'])['keyword'].count()
  print(grouped)

  if writeCSV:
    path = './output/keyword_count_by_location_grouped_by_hour.csv'
    outcsv = open(path, 'w')
    outcsv.truncate()
    writer = csv.writer(outcsv)
    writer.writerow(['month', 'day', 'hour', 'location', 'keyword', 'count'])
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