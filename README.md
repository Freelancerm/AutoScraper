# AutoScraper

**Asynchronous scraper for cars listings with PostgreSQL storage and scheduled jobs (scraping + DB dumps).**

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Run with Docker](#run-with-docker)
- [Run Locally](#run-locally)
- [Scheduler and Dumps](#scheduler-and-dumps)

## Overview
The app periodically scans search result pages, extracts listing URLs, parses listing details (title, price, mileage, VIN, photos, seller phone, etc.), and saves them to PostgreSQL. It also creates scheduled database dumps.

## Features
- Async scraping with concurrency limits and retries.
- Parsing key listing fields (price, mileage, VIN, plate number, photos, phone).
- Deduplication by URL (unique constraint).
- Scheduled scraping and DB dumps via APScheduler.
- DB dumps stored in `dumps/`.

## Tech Stack
- Python 3.11
- aiohttp, BeautifulSoup
- PostgreSQL + psycopg
- APScheduler
- Docker / docker-compose (optional)

## Project Structure
```
AutoScraper/
├─ src/
│  ├─ main.py        # app entry point and scheduler
│  ├─ scraper.py     # scraping/parsing logic
│  ├─ database.py    # DB access, inserts, dumps
│  ├─ scheduler.py   # scheduling logic
│  ├─ models.py      # Pydantic listing model
│  └─ config.py      # environment config
├─ dumps/            # DB dumps (mounted as volume in Docker)
├─ docker-compose.yaml
├─ Dockerfile
├─ requirements.txt
└─ example.env
```

## Configuration
Copy `example.env` to `.env` and adjust values.

Key variables:
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` - PostgreSQL credentials.
- `SCRAPE_START_URL` - search start URL (default `https://auto.ria.com/uk/car/used/`).
- `MAX_RETRIES` - request retry count.
- `MAX_CONCURRENCY` - parallel request limit.
- `TZ` - timezone (e.g., `Europe/Kyiv`).
- `SCRAPE_TIME` - daily scrape time in `HH:MM`.
- `DUMP_TIME` - daily dump time in `HH:MM`.
- `RUN_ON_STARTUP` - if set to any value, runs scraping immediately on startup.

## Run with Docker
1) Create `.env`:
```bash
cp example.env .env
```
2) Start services:
```bash
docker-compose up --build
```

Scraper logs are in the `app` container stdout. Dumps are written to `dumps/`.

## Run Locally
1) Create a virtual environment and install dependencies:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
2) Set environment variables (e.g., export from `.env`).
3) Run:
```bash
python -m src.main
```

Note: local runs require a reachable PostgreSQL instance and `pg_dump` available on the system (for dumps).

## Scheduler and Dumps
The scheduler runs:
- scraping at `SCRAPE_TIME`;
- DB dumps at `DUMP_TIME`.

Dumps are created using `pg_dump --format=custom` and stored in `dumps/` with timestamped filenames.
