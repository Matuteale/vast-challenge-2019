import pandas as pd
import csv

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
  user_count_by_location_grouped_by_hour(data, True)
  # print('\n\nCounting tweets by user...\n\n')
  # count_user_tweets(data)
  # print('\n\nCounting retweets by user...\n\n')
  # count_user_retweets(data)
  # print('\n\nCalculating users that tweet at different locations...\n\n')
  # users_that_tweet_at_diff_locations(data)



if __name__== '__main__':
  main()