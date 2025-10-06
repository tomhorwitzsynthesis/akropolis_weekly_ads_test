# Ads Tracking Dashboard

A system for tracking and analyzing Facebook ads from Lithuanian shopping centers and retail brands.

## Quick Start for Colleagues

### 1. View the Dashboard
```bash
streamlit run dashboard.py
```
This opens an interactive dashboard in your browser showing:
- 14-day performance analysis
- Weekly comparison metrics (current vs previous week)
- AI-generated summaries for each brand
- Top performing ads and clusters
- Interactive charts and filters

### 2. Understanding the Data
The system tracks Facebook ads from 21 brands including:
- **Akropolis locations**: Vilnius, Klaipėda, Šiauliai
- **Major competitors**: PANORAMA, OZAS, Kauno Akropolis
- **Smaller shopping centers**: BIG Vilnius, Vilnius Outlet, etc.
- **Retail chains**: Maxima LT, Lidl Lietuva, Rimi Lietuva, IKI

Interestingly enough, Kauno Akropolis is not part of the Akropolis franchise, so they are tracked separately.

### 3. Key Data Files
- `Akropolis_Ad_Updates/data/ads_master_file.xlsx` - All Facebook ads with engagement metrics and AI categories
- `Akropolis_Ad_Updates/data/summaries.xlsx` - Weekly AI-generated summaries for each brand

## Weekly Data Updates

### Manual Update 
Fill in the correct dates (ANALYSIS_START_DATE and ANALYSIS_END_DATE) in the config.py file, which should be a 14-day period. 
(For me it's always a bit counter-intuivite, but so 1/10 - 15/10 is not a 14 day period, it's actually 15 days, so it should be 1/10 - 14/10, just to be sure.)

It's a good idea to make copies of the output files in the Akropolis_Ad_Updates\data folder (ads_master_file and summaries) before you run the analysis, just to have a backup if something seems off.

If that's all done, run this command or simply go to the file in VS Code/Cursor and run it.
```bash
python pipeline.py
```

This will:
1. Scrape new Facebook ads from the last 14 days
2. Apply AI labeling to categorize ads
3. Merge with existing data (removing duplicates)
4. Generate updated weekly summaries

You will see a lot of messages, as the process is parallelized, so multiple queries are run at the same time. Warnings or errors are usually okay in these logs, if there is an actual error with the code the scraping will just stop and show the error.

The process of scraping can take 5-10 minutes.

If you want to just run the scraping/labeling/summarization or any combination of these 3, you can enable or disable them separately in the config file as well. This can be useful when for example changing the summary prompt and not wanting to redo the whole scraping, or to make summaries of dates in the past.

If something goes wrong with the scraping on Apify's end, you can check out the runs here: https://console.apify.com/actors/JJghSZmShuco4j9gJ/runs 

## What You Need to Run Updates

### Required API Keys
You need these in a `.env` file in the project folder:
```
APIFY_TOKEN=your_apify_token_here
OPENAI_API_KEY=your_openai_api_key_here
```

## After the Updates

After running all the analyses, it's good to check the ads_master_file to see if everything is okay. The format of the file might be looking a bit weird, as the way of scraping and analysis has changed throughout the development, so not all columns are filled in, but this is okay.

Check:
- If the startDateFormatted column has rows with the newly scraped dates
- If the ad_summary, cluster_1, cluster_2 and cluster_3 are filled in for those
- If the total_engagement column has values, it's normal that the comments and shares in those columns are 0
- If the summaries.xlsx file has a new row with the dates from the config file, and check quickly if the brands actually have summaries or that it says for a majority "This week there were no new posts" or something similar
- Run the dashboard and check if you can select the new dates and if the numbers in the summaries (number of ads and reach) correspond between the summaries and the metric cards at the top.


