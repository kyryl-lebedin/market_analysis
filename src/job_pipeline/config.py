from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from typing import Literal


#################################### Models #########################################


class Paths(BaseModel):
    project_root: Path
    data: Path
    bronze: Path
    silver: Path
    gold: Path
    logs: Path

    def ensure_dirs(self) -> None:
        for p in (self.bronze, self.silver, self.gold, self.logs):
            p.mkdir(parents=True, exist_ok=True)


class LoggingCfg(BaseModel):
    level: str = "INFO"
    app_name: str = "job_pipeline"

    @field_validator("level")
    @classmethod
    def _validate_level(cls, v: str) -> str:
        v = v.upper()
        allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}
        if v not in allowed:
            raise ValueError(
                f"Invalid LOG_LEVEL={v!r}. Allowed: {', '.join(sorted(allowed))}"
            )
        return v


Scope = Literal["single_page", "page_list", "all_pages"]
Mode = Literal["single_thread", "multithreading"]


class Preset(BaseModel):
    country: str = "gb"
    formatted: bool = Field(default=True)
    max_workers: int = 3
    what_or: str | None = None
    what_and: str | None = None
    mode: Mode | None = None
    scope: Scope | None = None
    page: int | None = None
    page_list: list[int] | None = None


#########################################################################################


class Settings(BaseSettings):

    # called load_dotenv already, so don't bother with .env
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    DATA_DIR: Path | None = None
    BRONZE_DIR: Path | None = None
    SILVER_DIR: Path | None = None
    GOLD_DIR: Path | None = None
    LOGS_DIR: Path | None = None

    LOG_LEVEL: str = "INFO"
    LOG_APP_NAME: str = "job_pipeline"

    # must set  variables
    ADZUNA_ID: str
    ADZUNA_KEY: str

    BD_HOST: str
    BD_PORT: int
    BD_USERNAME_BASE: str
    BD_PASSWORD: str
    BD_COUNTRY: str

    @property
    def paths(self) -> Paths:
        # insurance in case no env root variables
        project_root = Path(__file__).resolve().parents[2]

        data = (
            Path(self.DATA_DIR) if self.DATA_DIR else (project_root / "data")
        )  # use .env or alternatively create in proj root
        return Paths(
            project_root=project_root,
            data=data,
            # ensure proper roots if none added to .env
            bronze=Path(self.BRONZE_DIR) if self.BRONZE_DIR else (data / "bronze"),
            silver=Path(self.SILVER_DIR) if self.SILVER_DIR else (data / "silver"),
            gold=Path(self.GOLD_DIR) if self.GOLD_DIR else (data / "gold"),
            logs=Path(self.LOGS_DIR) if self.LOGS_DIR else (project_root / "logs"),
        )

    @property
    def logging(self) -> LoggingCfg:
        return LoggingCfg(level=self.LOG_LEVEL, app_name=self.LOG_APP_NAME)

    @property
    def adzuna_presets(self) -> dict[str, Preset]:
        # the code just written as list for convinience but stored as string
        WHAT_OR_EXTENSIVE_DATA = " ".join(
            [
                "data_scientist",
                "data_science",
                "data_engineer",
                "data_engineering",
                "big_data_engineer",
                "data_platform_engineer",
                "data_platform",
                "data_architect",
                "analytics_engineer",
                "data_analyst",
                "data_analytics",
                "business_intelligence",
                "bi_analyst",
                "bi_developer",
                "data_visualization",
                "data_visualisation",
                "analytics_consultant",
                "data_science_consultant",
                "etl_developer",
                "etl_engineer",
                "sql_developer",
                "data_warehouse_engineer",
                "data_warehousing",
                "dwh_engineer",
                "data_modeler",
                "data_modeller",
                "data_quality_engineer",
                "data_quality_analyst",
                "data_governance",
                "data_steward",
                "data_ops",
                "dataops",
                "streaming_data_engineer",
                "streaming_engineer",
                "kafka_engineer",
                "spark_engineer",
                "pyspark_developer",
                "databricks_engineer",
                "snowflake_engineer",
                "snowflake_developer",
                "dbt_developer",
                "machine_learning_engineer",
                "ml_engineer",
                "ai_engineer",
                "ai_developer",
                "mlops_engineer",
                "ml_ops",
                "model_ops",
                "modelops",
                "ml_platform_engineer",
                "machine_learning_platform_engineer",
                "ml_infrastructure_engineer",
                "research_engineer",
                "applied_scientist",
                "ai_scientist",
                "ml_scientist",
                "nlp_engineer",
                "natural_language_processing",
                "nlp_scientist",
                "computer_vision_engineer",
                "computer_vision",
                "cv_engineer",
                "speech_recognition",
                "speech_engineer",
                "audio_ml_engineer",
                "deep_learning_engineer",
                "deep_learning",
                "reinforcement_learning",
                "recommendation_systems_engineer",
                "recommender_systems",
                "personalization_engineer",
                "information_retrieval_engineer",
                "search_engineer",
                "relevance_engineer",
                "ranking_engineer",
                "generative_ai",
                "genai",
                "large_language_models",
                "llm_engineer",
                "llm_developer",
                "prompt_engineer",
                "prompt_engineering",
                "knowledge_graph_engineer",
                "graph_machine_learning",
                "quantitative_analyst",
                "quant_analyst",
                "decision_scientist",
                "product_data_scientist",
                "insights_analyst",
                "statistician",
                "time_series_analyst",
                "forecasting_analyst",
            ]
        )
        WHAT_OR_CORE_DATA = " ".join(
            ["data_scientist", "data_engineer", "machine_learning", "ai"]
        )
        WHAT_AND_680 = " ".join(["cat", "or", "and", "in", "of", "it"])
        WHAT_AND_1417 = " ".join(["cat", "ll"])

        return {
            "default": Preset(
                country="gb", formatted=True, max_workers=3, what_or="python django"
            ),
            "data_scientist_gb": Preset(
                country="gb",
                formatted=True,
                max_workers=3,
                what_and="data_scientist",
                mode="multithreading",
                scope="all_pages",
            ),
            "data_scientist_us": Preset(
                country="us",
                formatted=True,
                max_workers=3,
                what_and="data_scientist",
                mode="multithreading",
                scope="all_pages",
            ),
            "test_multithread_1417": Preset(
                country="gb",
                formatted=True,
                max_workers=3,
                scope="all_pages",
                what_and=WHAT_AND_1417,
                mode="multithreading",
            ),
            "test_single_page": Preset(
                country="gb",
                formatted=True,
                scope="single_page",
                what_and=WHAT_AND_1417,
                page=6,
            ),
            "test_page_list": Preset(
                country="gb",
                formatted=True,
                scope="page_list",
                mode="multithreading",
                max_workers=3,
                what_and=WHAT_AND_1417,
                page_list=[6, 7, 8, 9, 10],
            ),
            "extensive_or_search_gb": Preset(
                country="gb",
                formatted=True,
                max_workers=3,
                what_or=WHAT_OR_EXTENSIVE_DATA,
            ),
            "core_or_search_gb": Preset(
                country="gb", formatted=True, max_workers=3, what_or=WHAT_OR_CORE_DATA
            ),
        }


def get_settings() -> Settings:
    """factory to import singleton"""
    return Settings()
