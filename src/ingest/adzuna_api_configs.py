what_or_extensive_data = "data scientist,data science,data engineer,data engineering,big data engineer,data platform engineer,data platform,data architect,analytics engineer,data analyst,data analytics,business intelligence,bi analyst,bi developer,data visualization,data visualisation,analytics consultant,data science consultant,etl developer,etl engineer,sql developer,data warehouse engineer,data warehousing,dwh engineer,data modeler,data modeller,data quality engineer,data quality analyst,data governance,data steward,data ops,dataops,streaming data engineer,streaming engineer,kafka engineer,spark engineer,pyspark developer,databricks engineer,snowflake engineer,snowflake developer,dbt developer,machine learning engineer,ml engineer,ai engineer,ai developer,mlops engineer,ml ops,model ops,modelops,ml platform engineer,machine learning platform engineer,ml infrastructure engineer,research engineer,applied scientist,ai scientist,ml scientist,nlp engineer,natural language processing,nlp scientist,computer vision engineer,computer vision,cv engineer,speech recognition,speech engineer,audio ml engineer,deep learning engineer,deep learning,reinforcement learning,recommendation systems engineer,recommender systems,personalization engineer,information retrieval engineer,search engineer,relevance engineer,ranking engineer,generative ai,genai,large language models,llm engineer,llm developer,prompt engineer,prompt engineering,knowledge graph engineer,graph machine learning,quantitative analyst,quant analyst,decision scientist,product data scientist,insights analyst,statistician,time series analyst,forecasting analyst"
what_or_core_data = "data scientist,data engineer,machine learning,ai"

what_and_680 = "cat, or,and,in,of,it"
what_and_1411 = "cat,ll"

ADZUNA_API_PRESETS = {
    "default": dict(
        country="gb",
        formated=True,
        max_workers=3,
        what_or="python,django",
    ),
    "test_multithread_1411": dict(
        country="gb",
        formated=True,
        max_workers=3,
        scope="all_pages",
        what_and=what_and_1411,
        mode="multithreading",
    ),
    "test_single_page": dict(
        country="gb",
        formated=True,
        scope="single_page",
        what_and=what_and_1411,
        page=6,
    ),
}
