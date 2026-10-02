from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Single source of truth for configuration.

    Values come from environment variables (or .env), never from hardcoded
    literals. Hardcoding localhost would force every component onto one box,
    which makes the word "distributed" false.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://taskforge:taskforge@localhost/taskforge"

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0

    # Invariant: worker_ttl > heartbeat_interval * 2
    # One dropped packet must not be enough to declare a healthy worker dead.
    # 3x tolerates two consecutive misses.
    heartbeat_interval: int = 5

    max_retries: int = 3
    
    @property
    def worker_ttl(self) -> int:
        return self.heartbeat_interval * 3

    # How long a task may sit IN_PROGRESS before the reaper considers it
    # abandoned, independent of worker liveness.
    task_lease_seconds: int = 60

    reaper_interval: int = 5

    scheduler_interval: int = 10

settings = Settings()
