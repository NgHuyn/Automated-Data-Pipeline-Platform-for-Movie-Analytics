# ETL-Data-Pipeline-for-Aspect-based-sentiment-analysis
## Table of contents :pushpin:
- [Overview](#overview)
- [Prerequisite](#prerequisite)
- [Features](#features)
- [Installation](#installation)
    - [Set up environment](#set-up-environment)
    - [Run your data pipeline](#run-your-data-pipeline)
    - [Dashboard](#power-bi-dashboard)
- [Future Work](#future-work)
- [Contributors](#contributors)

## Overview
This project is designed to build an ETL (Extract, Transform, Load) data pipeline for analyzing and building ABSA model for movie reviews using data from IMDB and The Movie Database (TMDB) API. It involves fetching movie-related data, processing it, and storing it in a relational database.

### Project structure

### Design architecture
We will use Docker for conternize framework and Prefect for orchestrate tasks.
1. We will use IMDB for crawling review data, then base on the IMDB id of each movie, we will you TMDB API to find the movie and get more information of each movie such as: genre, actor, director,...
2. Extract data from 2 source to MongoDB Atlas - also the staging area
3. We will use Python to transform it directly from MongoDB Atlas
4. Load the cleaned data into PostgreSQL
5. Build dashboard by using Power BI
![Achitecture](./image/data_pipeline.png)

### Data Schema 
This is our schema .... 
![Schema](./image/data_schema.png)

## Prequisites
To use this pipeline, we need some prequisites:
- API key from TMDB: [The Movie Database API account](https://developer.themoviedb.org/docs/getting-started)
- Docker with docker compose: [Docker](https://www.docker.com/products/docker-desktop/)
- MongoDB Atlas account: [MongoDB Atlas](https://www.mongodb.com/cloud/atlas/register)

## [Features](#features)
In this project, we create 2 deployment corresponding to 2 pipelines: manually-ETL-pipeline & ETL-pipeline.
- manually-ETL-pipeline: for getting historical data and will be triggered manually when we enter parameters are release_date_from and release_date_to
- ETL-pipeline: will automatically run each every 7 days from the first day we set in .env file, when it run, first it will get all the movie data from last 7 days, after that it will check the top 10 most popular movie of the last 7 days base on its popularity point to update new reviews of each movie. (If we run the first time it will create and skip this check and update part). And after update have succesfully, it will update this week's new top 10.

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
This data flow (or pipeline) is used to scrape data from IMDB and then call TMDB API and ingest into
MongoDB Atlas. You can trigger it to get historical data (or not).
<div style="display: flex; justify-content: space-between;">

![pipeline1-a](./image/pipline1-a.jpg)

![pipeline1-b](./image/pipline1-b.jpg)

</div>

#### Pipeline 2 (ETL pipeline)
This pipepline will be our main pipeline, which is automatically updated every 7 days...

<div style="display: flex; justify-content: space-between;">

![pipline2-a](./image/pipeline2-a.jpg)

![pipline2-b](./image/pipeline2-b.jpg)

### Power BI Dashboard
![powerbi](./image/powerbi-dash.jpg)
> You can also see it in [powerbi_dashboard](./dashboard/powerbi_dashboard.pdf)

## Future Work
In future, we will update this repo in: 
- Utilizing machine learning model: In the future, we will use reviews of movies to create aspect-based sentiment analysis model...
- ...
## Contributors
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/NgHuyn"><img src="https://avatars.githubusercontent.com/u/94174684?v=4" width="100px;" alt="Nguyen Hai Ngoc Huyen"/><br /><sub><b>Nguyen Hai Ngoc Huyen</b></sub></a><br /> Data Analyst </td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/KimThy13"><img src="https://avatars.githubusercontent.com/u/94174684?v=4" width="100px;" alt="Ta Hoang Kim Thy"/><br /><sub><b>Ta Hoang Kim Thy</b></sub></a><br /> Data Analyst </td>
    </tr>
  </tbody>
</table>
