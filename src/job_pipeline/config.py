from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()

# Project structure
PROJECT_ROOT = Path(__file__).parent.parent.parent


# Data directories - can be overridden by environment variables
DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))
BRONZE_DIR = Path(os.getenv("BRONZE_DIR", DATA_DIR / "bronze"))
SILVER_DIR = Path(os.getenv("SILVER_DIR", DATA_DIR / "silver"))
GOLD_DIR = Path(os.getenv("GOLD_DIR", DATA_DIR / "gold"))
LOGS_DIR = Path(os.getenv("LOGS_DIR", PROJECT_ROOT / "logs"))

BRONZE_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# API Configuration
ADZUNA_ID = os.getenv("ADZUNA_ID")
ADZUNA_KEY = os.getenv("ADZUNA_KEY")

BD_HOST = os.getenv("BD_HOST")
BD_PORT = os.getenv("BD_PORT")
BD_USERNAME_BASE = os.getenv("BD_USERNAME_BASE")
BD_PASSWORD = os.getenv("BD_PASSWORD")
BD_COUNTRY = os.getenv("BD_COUNTRY")


######## ADZUNA API PRESETS ##########

what_or_extensive_data = "data scientist,data science,data engineer,data engineering,big data engineer,data platform engineer,data platform,data architect,analytics engineer,data analyst,data analytics,business intelligence,bi analyst,bi developer,data visualization,data visualisation,analytics consultant,data science consultant,etl developer,etl engineer,sql developer,data warehouse engineer,data warehousing,dwh engineer,data modeler,data modeller,data quality engineer,data quality analyst,data governance,data steward,data ops,dataops,streaming data engineer,streaming engineer,kafka engineer,spark engineer,pyspark developer,databricks engineer,snowflake engineer,snowflake developer,dbt developer,machine learning engineer,ml engineer,ai engineer,ai developer,mlops engineer,ml ops,model ops,modelops,ml platform engineer,machine learning platform engineer,ml infrastructure engineer,research engineer,applied scientist,ai scientist,ml scientist,nlp engineer,natural language processing,nlp scientist,computer vision engineer,computer vision,cv engineer,speech recognition,speech engineer,audio ml engineer,deep learning engineer,deep learning,reinforcement learning,recommendation systems engineer,recommender systems,personalization engineer,information retrieval engineer,search engineer,relevance engineer,ranking engineer,generative ai,genai,large language models,llm engineer,llm developer,prompt engineer,prompt engineering,knowledge graph engineer,graph machine learning,quantitative analyst,quant analyst,decision scientist,product data scientist,insights analyst,statistician,time series analyst,forecasting analyst"
what_or_core_data = "data scientist,data engineer,machine learning,ai"

what_and_680 = "cat, or,and,in,of,it"
what_and_1417 = "cat,ll"

ADZUNA_API_PRESETS = {
    "default": dict(
        country="gb",
        formated=True,
        max_workers=3,
        what_or="python,django",
    ),
    "data_scientist_gb": dict(
        country="gb",
        formated=True,
        max_workers=3,
        what_and="data_scientist",
        mode="multithreading",
        scope="all_pages",
    ),
    "data_scientist_us": dict(
        country="us",
        formated=True,
        max_workers=3,
        what_and="data_scientist",
        mode="multithreading",
        scope="all_pages",
    ),
    "test_multithread_1417": dict(
        country="gb",
        formated=True,
        max_workers=3,
        scope="all_pages",
        what_and=what_and_1417,
        mode="multithreading",
    ),
    "test_single_page": dict(
        country="gb",
        formated=True,
        scope="single_page",
        what_and=what_and_1417,
        page=6,
    ),
    "test_page_list": dict(
        country="gb",
        formated=True,
        scope="page_list",
        mode="multithreading",
        max_workers=3,
        what_and=what_and_1417,
        page_list=[6, 7, 8, 9, 10],
    ),
}
