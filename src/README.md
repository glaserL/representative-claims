# Usage

This contains everything that was used to collect and automatically annotate the data we collected from Twitter. Because we are not allowed to distributed tweets ourselves, we only provide a script that will crawl those tweets for you. A list of IDs is contained in in `../data/tweet_data.zip`. If you want to access that data, feel free to send us an eMail and we will send you the password. 

## Requirements

All required python packages can be installed with the `requirements.txt`:
```bash
pip install -r requirements.txt
```

In case you have never used spacy with German before, download the model: 
```bash
python -m spacy install de_core_news_sm
```

## Steps to crawl

We prepared scripts that will process these ids for you and generate the data we worked with. You will need a Twitter Developer Account to collect the data. If you want to access all constituencies of the users, you may require an account for [genderize.io](genderize.io) to lift the rate limits. Put both access informations into `config.json`.

Execute them in the following order using python3 (from the `src` directory):

```bash
python get_tweets.py;
python get_hydrated_userobjects.py;
python annotate_location.py;
python collect_gender_annotations;
python merge_userobjects_genders;
python write_data_to_graph.py --commands all;
```


## Acknowledgements

* The crawling of old tweets was performed with the [GetOldTweets](https://github.com/Jefferson-Henrique/GetOldTweets-python), which we slightly modified.
* The sentiment annotations were done with the [VADER sentiment analysis](https://github.com/cjhutto/vaderSentiment) package, which we slightly m