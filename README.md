# Twitter-Tweets-Collection-And-PostProcessing
Twitter tweets collection and post-processing, including keyword filtering, variables cleaning up, sentiment testing etc. Please cite us if you use any of the code or the model.

Please follow the steps for tweets collection and post processing. We requested tweets via Twitter Academic Research API. For more information, please go to:
https://developer.twitter.com/en/products/twitter-api/academic-research

**Step 1: run Historical_Tweets_Requests.ipynb**

This code allows you to make API request with customized parameters and download tweets.

**Step 2: run Collected_Tweets_Filtering.py**

This code extracts key terms from tweets texts and stores them and their categories into new variables. The code also filters out irrelevant tweets, which are tweets that don't contain any key terms.

**Step 3: run clean_geolocation_variable_add_fips.Rmd**

This code cleans geolocation variables and adds state and county fips as new variables.

**Step 4: run sentiment_prediction_example.py from Sentiment model folder**

This code runs a pre-trained sentiment model on each tweet, giving binary outputs for "whether sad" and "whether happy" based on the tweet's content.


