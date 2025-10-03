# Ads Tracking Dashboard - Weekly Update Guide

This dashboard tracks advertising performance for Akropolis shopping centers and their competitors in Lithuania. It scrapes Facebook ads data, processes it with AI labeling, and generates weekly summaries for analysis.

## Weekly Data Update Process

### Prerequisites

1. **API Keys Required:**
   - Apify API token (for Facebook ads scraping)
   - OpenAI API key (for AI labeling and summaries)

2. **Environment Setup:**
   ```bash
   # Install dependencies
   pip install -r requirements.txt
   
   # Create .env file with your API keys
   APIFY_TOKEN=your_apify_token_here
   OPENAI_API_KEY=your_openai_api_key_here
   ```

### Weekly Update Steps

#### 1. Run the Data Pipeline
First, make simple copies of `/Akropolis_Ad_Updates/data/ads_master_file.xlsx` and `./Akropolis_Ad_Updates/data/summaries.xlsx` in case something goes wrong.

Execute the main pipeline to scrape new ads data:

```bash
python pipeline.py
```

**What this does:**
- Scrapes Facebook ads from 19 competitor pages using Apify
- Processes and cleans the data
- Applies AI labeling to categorize ads
- Updates the master Excel file with new data
- Generates weekly AI summaries for all brands

**Configuration (in `config.py`):**
- `DAYS_BACK = 14` - How many days back to scrape
- `MAX_ADS = 50` - Max ads per page
- `ENABLE_GPT_LABELING = True` - Enable AI categorization
- `ENABLE_WEEKLY_SUMMARIES = True` - Generate AI summaries

#### 2. Verify Data Update
Check that new data was added:
- Master file: `./Akropolis_Ad_Updates/data/ads_master_file.xlsx`
- Summaries: `./Akropolis_Ad_Updates/data/summaries.xlsx`
Especially check if the summaries have a new row

#### 3. Run the Dashboard
Launch the Streamlit dashboard:

```bash
streamlit run dashboard.py
```

The dashboard shows:
- 14-day performance analysis
- Weekly comparison metrics (current vs previous week)
- AI-generated summaries for each brand
- Top performing ads and clusters
- Interactive charts and filters

## Double-Check in the Dashboard
- See if the metrics, number of ads and reach, correspond between the summaries and the metric cards at the top.

## Data Flow

1. **Scraping** → Facebook ads from 19 competitor pages
2. **Processing** → Clean, deduplicate, and transform data
3. **AI Labeling** → Categorize ads using GPT-4
4. **Storage** → Save to master Excel file
5. **Summaries** → Generate weekly AI summaries
6. **Dashboard** → Display interactive analysis

## Troubleshooting

### Common Issues

1. **API Rate Limits:**
   - Reduce `MAX_WORKERS` in config.py
   - Check Apify account usage limits

2. **Missing Data:**
   - Verify API keys in .env file
   - Check internet connection
   - Ensure target Facebook pages are public

3. **Dashboard Not Loading:**
   - Run `streamlit run dashboard.py` from project root
   - Check that data files exist in correct locations

### Performance Tips

- Run scraping during off-peak hours
- Monitor API usage to avoid rate limits
- Consider reducing `MAX_ADS` if scraping is slow
- Use `ENABLE_GPT_LABELING = False` to skip AI processing for faster updates

## Weekly Checklist

- [ ] Run `python pipeline.py` to update data
- [ ] Verify new data in Excel files
- [ ] Launch dashboard with `streamlit run dashboard.py`
- [ ] Review AI summaries for insights
- [ ] Check for any new competitors to add
- [ ] Monitor API usage and costs

## Support

For issues or questions about the dashboard setup, check:
- API key configuration in .env file
- File paths in config.py for your deployment
- Dependencies in requirements.txt
- Console output for error messages
