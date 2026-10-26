from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    max_concurrent_permits: int = 3000
    acquire_timeout_sec: float = 0.25
    queue_capacity: int = 5000

    class Config:
        env_prefix = "CONCURRENCY_"

settings = Settings()
