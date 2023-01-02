# ILTB

Simple scraper to scrape Invest Like the Best podcast episodes from [Join Colossus](https://www.joincolossus.com/).
The purpose is to use this data to fine-tune a GPT3 model. Just wanted to see how it works on a dataset that I had
familiarity with.

Basically:

1. Scraper gets the html and saves to s3
2. Parser gets the html from s3 and parses it into a json file and saves to s3
3. Notebooks get the json file from s3 and format the json into a pandas dataframe to be converted into a file
   format that works with openai protocol