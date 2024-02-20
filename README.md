# Premier League Table Scraper with Airflow and Google BigQuery

This project is designed to scrape the Premier League table data, schedule the scraping job using Apache Airflow, and ingest the data into Google BigQuery. The infrastructure for BigQuery is set up using Terraform, and Airflow is containerized using Docker and orchestrated using a custom docker-compose file.


### Overview

The project uses Apache Airflow to schedule the Premier League table scraping job, which is implemented as an Airflow DAG (Directed Acyclic Graph). The scraped data is then ingested into Google BigQuery for further analysis and visualization.

### Project Structure

- `scrapper.py`: This file contains the Python code for scraping the Premier League table data from the web.
- `dags/`: This directory contains the Airflow DAG file for scheduling the scraping of the Premier League table data.
- `main.tf`: This file contains the Terraform configuration files for setting up the infrastructure on GCP for BigQuery.
- `docker-compose.yml`: This file contains the Docker Compose configuration for running the Airflow web server and scheduler.

### Prerequisites

- Google Cloud Platform (GCP) Account: Ensure you have a GCP account with the necessary permissions to create BigQuery datasets and tables.
- Terraform: Install Terraform on your local machine. Visit Terraform Downloads for installation instructions.
- Docker: Install Docker on your local machine. Visit Docker for installation instructions.
- Docker Compose: Install Docker Compose on your local machine. Visit Docker Compose for installation instructions.


### Installation

1. Clone the repository to your local machine:

```bash
git clone https://github.com/Ishan-phys/league-table-scraper.git
cd league-table-scraper
```

2. Create a new project on GCP and enable the BigQuery API.
Set up your Google Cloud credentials: Follow the [Google Cloud Authentication documentation](https://developers.google.com/workspace/guides/create-credentials) to set up your credentials. Make sure the credentials have the necessary permissions for BigQuery.

3. Configure the Airflow environment:
Update the `.env` file with your Google Cloud credentials and desired Airflow configurations.

4. Build and start the Docker containers:

```bash
docker-compose up -d
```

### Project Structure

```
premier-league-scraper/
│
├── dags/
│   └── scraper.py
│   └── data_ingestion_dag.py
│   └── upload_to_gcs.py
│   └── upload_postgres.py
│
├── main.tf
├── variables.tf
│
├── docker-compose.yml
├── .env
└── ...
```

### Configuration

The `.env` file contains the environment variables for the Airflow web server and scheduler. Update the `.env` file with your Google Cloud credentials and desired Airflow configurations.

```bash
# Google Cloud credentials file path
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/credentials.json

# Google Cloud project ID
GOOGLE_PROJECT_ID=your-project-id

# Airflow configuration
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin
AIRFLOW_API_AUTHENTICATE=true
AIRFLOW_WEB_SERVER_PORT=8080
```

### Usage

1. Run the Airflow web server and scheduler:

```bash
docker-compose up -d
```

2. Access the Airflow web interface in your web browser:

Open a web browser and navigate to `http://localhost:8080`. Log in using the credentials specified in the .env file.

3. Trigger the Premier League table scraping DAG manually or wait for the scheduled run.

4. Monitor the progress of the scraping job in the Airflow web interface.


### License
This project is licensed under the MIT License.