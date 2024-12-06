# ETL-Data-Pipeline-for-Aspect-Based-Sentiment-Analysis

## Table of contents :pushpin:
- [Overview](#overview)  
- [Prerequisites](#prerequisites)  
- [Features](#features)  
- [Installation](#installation)  
    - [Set up environment](#set-up-environment)  
    - [Run your data pipeline](#run-your-data-pipeline)  
    - [Dashboard](#power-bi-dashboard)  
- [Future Work](#future-work)  
- [Contributors](#contributors)  

---

## Overview  

This project builds an ETL (Extract, Transform, Load) data pipeline for analyzing and developing an **Aspect-Based Sentiment Analysis (ABSA)** model for movie reviews. By leveraging data from IMDB and The Movie Database (TMDB) API, this pipeline automates the collection, processing, and storage of movie-related data in a structured format.

### Project Structure
- **Data Sources**: Data is extracted from IMDB (reviews) and TMDB (movie details).  
- **Data Pipeline**:  
  1. Extract: Gather data from IMDB and TMDB APIs.  
  2. Transform: Process and clean data using Python.  
  3. Load: Store transformed data in PostgreSQL.  
- **Visualization**: Build interactive dashboards in Power BI for insights.  

### Design Architecture
This project uses Docker for containerization and Prefect for task orchestration.  
Steps:  
1. Crawl reviews from IMDB, then fetch additional movie details (genre, actors, directors) via TMDB API.  
2. Extract and load raw data into **MongoDB Atlas** (staging area).  
3. Transform data in Python directly from MongoDB Atlas.  
4. Load cleaned data into **PostgreSQL**.  
5. Build Power BI dashboards for insights.  

![Architecture](./image/data_pipeline.png)  

### Data Schema  
The schema outlines the relationships between movies, reviews, actors, and providers.  
![Schema](./image/data_schema.png)  

---

## Prerequisites  

Before running the pipeline, ensure you have:  
- **TMDB API Key**: Create an account and get your API key from [The Movie Database](https://developer.themoviedb.org/docs/getting-started).  
- **Docker**: Install Docker Desktop, including Docker Compose ([Download Docker](https://www.docker.com/products/docker-desktop/)).  
- **MongoDB Atlas Account**: Register for free [here](https://www.mongodb.com/cloud/atlas/register).  

---

## Features  

This project includes two main pipelines:  
1. **Manually-ETL-Pipeline**:  
   - Triggered manually to fetch historical data based on user-provided date ranges (`release_date_from`, `release_date_to`).  
2. **ETL-Pipeline**:  
   - Automatically runs every 7 days.  
   - Updates weekly data and checks the top 10 most popular movies of the last 7 days based on popularity scores to fetch new reviews.  

---

## Installation  

### Set up environment  
Clone this repository:
```bash
git clone [https://github.com/NgHuyn/ETL-Data-Pipeline-for-ABSA.git
cd ETL-Data-Pipeline-for-ABSA
```
then create `.env` file base on [env_template](./env_template)
```bash
cp env_template .env
```
Fill your informations blank in `.env` file, this can be done in [Prerequisite](#prerequisite).
And now, we can run our pipline! Let's build your Docker images of this project by typing `make build` in your
terminal 

Note: if you don't have WSL(Ubuntu) in your terminal, you can install it to you `make-`. Or just use the corresponding replace statement in the [Makefile](./Makefile)

> This process might take a few minutes, so just chill and take a cup of coffee :coffee:

**Note: if you failed in this step, just remove the image or restart Docker and try again**

If you've done building Docker images, now its time to run your system. Just type `make up` 

Then check your services to make sure everything work correctly:
1. Prefect
    - [`localhost:4200`](http://localhost:4200/): Prefect Server
2. pgAdmin
    - [`localhost:5050`](http://localhost:5050/): pgAdmin
  
![docker_container](./image/docker_container.jpg)
### Run your data pipeline
We use [Prefect](https://www.prefect.io/) to build our data pipeline. When you check out port `4200`, you'll see
prefect UI, let's go to Deployment section, you'll see 2 deployments there correspond to 2 data pipelines
#### Pipeline 1 (Manually ETL Pipeline)
- Use this pipeline to fetch historical data.
- Trigger it manually by entering the desired date range.
<div style="display: flex; justify-content: space-between;">

![pipeline1-a](./image/pipeline1-a.jpg)

![pipeline1-b](./image/pipeline1-b.jpg)

</div>

#### Pipeline 2 (ETL pipeline)
- Runs automatically every 7 days to update movie data and reviews.
- Initially fetches all movie data; subsequently, it updates the top 10 most popular movies weekly.

<div style="display: flex; justify-content: space-between;">

![pipline2-a](./image/pipeline2-a.jpg)

![pipline2-b](./image/pipeline2-b.jpg)

### Power BI Dashboard
![powerbi](./image/powerbi-dash.jpg)
> You can also see it in [powerbi_dashboard](./dashboard/powerbi_dashboard.pdf)

## Future Work
Planned updates include:
- Aspect-Based Sentiment Analysis Model: Build and integrate ABSA models using movie reviews.
- Real-Time Updates: Implement near-real-time review analysis.
- Advanced Visualization: Create dynamic visualizations for movie trends and audience sentiment.
## Contributors
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/NgHuyn"><img src="https://avatars.githubusercontent.com/u/94596692?v=4" width="100px;" alt="Nguyen Hai Ngoc Huyen"/><br /><sub><b>Nguyen Hai Ngoc Huyen</b></sub></a><br /> Data Analyst </td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/KimThy13"><img src="https://avatars.githubusercontent.com/u/92136844?v=4" width="100px;" alt="Ta Hoang Kim Thy"/><br /><sub><b>Ta Hoang Kim Thy</b></sub></a><br /> Data Analyst </td>
    </tr>
  </tbody>
</table>
