# Market Analysis Pipeline

A job scraping pipeline that collects job data from Adzuna API and processes it through bronze/silver/gold data layers. Currently deployed on AWS ECS Fargate for scalable batch processing.

## Scraping Details

### Data Sources
- **Primary**: Adzuna API for job listings
- **Secondary**: Web scraping individual job pages for full descriptions
- **Proxy**: Bright Data residential proxy for reliable scraping

### Scraping Process
1. **API Calls**: Fetch job listings from Adzuna with search filters
2. **URL Extraction**: Extract home URLs from job listings
3. **Page Scraping**: Visit individual job pages to get full descriptions
4. **Data Enrichment**: Combine API data with scraped content

### Search Capabilities
- **Countries**: GB, US, and others supported by Adzuna
- **Keywords**: AND/OR logic for job title searches
- **Scope**: Single page, page list, or all available pages
- **Concurrency**: Multithreading support (configurable max workers)

### Rate Limiting & Reliability
- Uses Bright Data proxy rotation for IP diversity
- Configurable concurrency to avoid overwhelming servers
- Retry logic for failed requests
- Structured logging for monitoring and debugging

## What it does

- Scrapes job listings from Adzuna API using various search parameters
- Processes raw data through bronze → silver → gold transformation pipeline
- Stores results in S3 (cloud) or local filesystem
- Supports custom search parameters and different data processing presets

## Quick Start

### Local Development
```bash
# Install dependencies
poetry install

# Run with preset
python -m job_pipeline.cli --preset test_page_list --name my_run

# Run with custom search
python -m job_pipeline.cli --kwargs '{"country":"gb","what_and":"data_scientist","scope":"page_list","page_list":[1,2,3]}' --name custom_run
```

### Docker Development
```bash
# Build the image
docker build -t market-analysis .

# Run with docker-compose
docker-compose up

# Or run directly
docker run --env-file .env market-analysis --preset test_page_list --name docker_run
```

### AWS ECS Deployment

The pipeline is deployed on ECS Fargate for production runs. Use one-time tasks for batch processing:

```bash
# Basic run
aws ecs run-task \
  --cluster market-analysis-cluster \
  --task-definition market-analysis-task \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-071838b9ab2d4148f],assignPublicIp=ENABLED}" \
  --region eu-west-2

# Custom search
aws ecs run-task \
  --cluster market-analysis-cluster \
  --task-definition market-analysis-task \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-071838b9ab2d4148f],assignPublicIp=ENABLED}" \
  --overrides '{
    "containerOverrides": [
      {
        "name": "market-analysis-container",
        "command": [
          "--kwargs",
          "{\"country\":\"gb\",\"what_and\":\"data_scientist\",\"scope\":\"page_list\",\"page_list\":[1,2,3]}",
          "--name",
          "custom_run"
        ]
      }
    ]
  }' \
  --region eu-west-2
```

## Configuration

- **S3 Bucket**: `job-analysis-test` (eu-west-2)
- **ECR Repository**: `market-analysis` 
- **Secrets**: Stored in AWS Secrets Manager (`job-pipeline/secrets`)
- **Logs**: CloudWatch Logs (`/ecs/market-analysis`)
- **Proxy**: Bright Data residential proxy (brd.superproxy.io:33335)

## Data Flow

1. **Bronze**: Raw API responses from Adzuna
2. **Silver**: Cleaned and structured data with home URLs
3. **Gold**: Full job descriptions and enriched data

## Search Examples

```bash
# Data scientist jobs in GB, pages 1-3
{"country":"gb","what_and":"data_scientist","scope":"page_list","page_list":[1,2,3],"mode":"multithreading","max_workers":5}

# Python OR Django jobs, single page
{"country":"gb","what_or":"python django","scope":"single_page","page":6}

# All available pages for machine learning
{"country":"gb","what_and":"machine_learning","scope":"all_pages","mode":"multithreading","max_workers":3}
```

## Notes

- Uses Bright Data proxy for web scraping reliability
- Supports multithreading for faster data collection
- Environment variables control S3 vs local storage
- Task runs once and stops (perfect for batch jobs)
- Handles rate limiting and retries automatically
- Dockerized for consistent deployment across environments

## Useful Commands

```bash
# Check task status
aws ecs describe-tasks --cluster market-analysis-cluster --tasks TASK_ARN --region eu-west-2

# List S3 results
aws s3 ls s3://job-analysis-test/ --recursive

# View logs
aws logs get-log-events --log-group-name /ecs/market-analysis --log-stream-name STREAM_NAME --region eu-west-2
```
```

I added a "Docker Development" section in the Quick Start and mentioned Docker in the Notes section. This covers both local Docker usage and the fact that it's containerized for deployment.