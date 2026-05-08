# TGSC Scraper

Scrapes substance information and demo formulations from [The Good Scents Company](https://www.thegoodscentscompany.com) into a SQLite database.

## Requirements

```bash
pip install requests beautifulsoup4 lxml
```

## Usage

The scraper works in two phases: **discover** (find URLs) and **scrape** (fetch and parse pages).

### Full run (all steps)

```bash
# Step 1: Discover all substance URLs from index pages
python3 tgsc_scraper.py discover-substances

# Step 2: Discover which demo formulation pages exist
python3 tgsc_scraper.py discover-demos

# Step 3: Scrape everything and build the database
python3 tgsc_scraper.py scrape-all
```

`scrape-all` scrapes substances and demos, then links demo ingredients to substances and builds full-text search indexes.

### Step-by-step alternative

```bash
python3 tgsc_scraper.py discover-substances
python3 tgsc_scraper.py discover-demos
python3 tgsc_scraper.py scrape-substances
python3 tgsc_scraper.py scrape-demos
python3 tgsc_scraper.py finalize
```

### Testing a single page

```bash
python3 tgsc_scraper.py test-substance /data/rw1015272.html
python3 tgsc_scraper.py test-demo /demos/dm1015272.html
```

## Resuming after a connection loss

If the scraper is interrupted (connection lost, timeout, Ctrl+C), progress is saved automatically. Add `--resume` to continue where you left off:

```bash
python3 tgsc_scraper.py scrape-substances --resume
python3 tgsc_scraper.py scrape-demos --resume
```

Or for the combined command:

```bash
python3 tgsc_scraper.py scrape-all --resume
```

The scraper tracks completed and failed URLs in progress files (`substance_scrape_progress.json`, `demo_scrape_progress.json`). On resume, only unprocessed URLs are fetched.

After all scraping is done, run `finalize` to link demo ingredients to substances and rebuild FTS indexes:

```bash
python3 tgsc_scraper.py finalize
```

## Output

`tgsc_perfume_resources.sqlite` with the following tables:

| Table | Description |
|---|---|
| `substances` | Substance data (name, CAS, formula, odor type, etc.) |
| `demos` | Demo formulation metadata (name, patent, application) |
| `demo_ingredients` | Ingredients per demo (name, amount, linked to substances) |
| `substances_fts` | Full-text search on substances |
| `demos_fts` | Full-text search on demos |

## Commands reference

| Command | Description |
|---|---|
| `discover-substances` | Find all substance URLs from TGSC index pages |
| `discover-demos` | Find which demo pages exist (HEAD requests) |
| `scrape-substances` | Scrape substance pages into SQLite |
| `scrape-demos` | Scrape demo pages into SQLite |
| `scrape-all` | Scrape both + finalize (link & build FTS) |
| `finalize` | Link demo ingredients to substances, build FTS |
| `test-substance <url>` | Parse and print a single substance page |
| `test-demo <url>` | Parse and print a single demo page |

All scrape commands accept `--resume`.
