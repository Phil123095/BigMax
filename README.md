# BigMax - A Massively Parallelized API Puller & Crypto Database

## TL;DR (the part I am most proud of): 
- I built a database using AWS Lambda functions & Python. I pull data from [Coingecko](www.coingecko.com) and [Coinmarketcap](www.coinmarketcap.com).
- Coingecko has a lot of interesting historical data for every crypto, such as Github activity, Reddit activity, Twitter, etc. However, all of this data is (normally) behind a paywall and an API rate limit (50 calls per minute for the free tier).
    - To pull all of the historical data, you would need to run an API call for each Coin-Day combination, which currently totals to roughly 4 million calls. 
    - At 50 calls per minute, that would take roughly.... **55 days**. 
- Unfortunately for Coingecko though, the rate limiting is done by IP address, and not through API key based accounting :(. So with a bit of multiprocessing magic, and some IP rotating wizardry, the rate limit can be bypassed. 
    - Meaning in terms of raw API query speed, I've gone from 50 calls per minute to roughly 500K calls per minute (tested with a good internet connection, and a M1 Max MBPro). 
    - Meaning I go from 55 days down to 8 minutes :) (legal fine print: this does not include actual uploading to my Database. With writing to the DB, it takes roughly 1.5 hours. Still pretty good).

- Next Step: 
  - Building the massively parallel part of the code into a framework/wrapper to work with any API that uses IP-based rate-limiting (I'm guessing there aren't many of them out there).
  - Using all the data I've collected, and building a cloud-based ML pipeline for individual crypto price predictions. 

## Enough Bragging, give me the actual documentation:

---- Coming Soon ----
