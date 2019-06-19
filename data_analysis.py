import pandas as pd

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

def main():
  print("\n\nReading csv data...\n\n")
  data = pd.read_csv("./data/MC3/Yint.csv")
  print("\n\nCounting tweets by user...\n\n")
  count_user_tweets(data)
  print("\n\nCounting retweets by user...\n\n")
  count_user_retweets(data)
  print("\n\nCalculating users that tweet at different locations...\n\n")
  users_that_tweet_at_diff_locations(data)


if __name__== "__main__":
  main()