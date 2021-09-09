# Data Pipeline PostgresSQL

## **Overview**
In this project we build ETL Pipeline using Python. Data concerns recent Tweets from Twitter that mention 'The Sims' video game, as it can be useful to know how popular are those tweets and what other products are mentioned. We gather recent tweets ourselves using Twitter API.
## **Sample Tweet data**
```
"data": [
		{
			"author_id": "1375164196535603203",
			"id": "1435628394880937987",
			"context_annotations": [
				{
					"domain": {
						"id": "71",
						"name": "Video Game",
						"description": "A video game like Overwatch"
					},
					"entity": {
						"id": "10046004953",
						"name": "The Sims",
						"description": "This entity includes all conversation about the franchise, as well as any individual installments in the series, if applicable. "
					}
				}
			],
			"public_metrics": {
				"retweet_count": 0,
				"reply_count": 0,
				"like_count": 0,
				"quote_count": 0
			},
			"lang": "pt",
			"text": "@1luvmyg Os banheiros na frente do refeitório tbm eram menores na minha cabeça, as cabines de xixi são espaçosas, mas de novo... O the sims não deixou fazer do jeito q eu pensei kkkk",
			"created_at": "2021-09-08T15:37:36.000Z",
			"possibly_sensitive": false
		},
"includes": {
		"users": [
			{
				"name": "Tapy aus 🍉",
				"verified": false,
				"username": "TapyHopeAus",
				"created_at": "2021-03-25T19:16:41.000Z",
				"id": "1375164196535603203"
```

## **Database schema**
#### Fact Table
**Tweets** - data associated with the tweet
```
text, tweet_id, author_id, possibly_sensitive, language, date_of_creation
```
#### Dimension Tables
**Authors** - data associated with author's account of the tweet
```
username, created_at, verified, name, author_id
```
**Public_metrics** - data associated with 'popularity' of the tweet
```
tweet_id, retweets_count, replies_count, like_count, quote_count,	
```
**Context_annotations** - data associated with products, domains, entites etc. which name was used in the tweet other than 'The Sims'
```
tweet_id, domain_name, entity_name
```
