import os
from dotenv import load_dotenv
from loguru import logger

# Load environment variables from .env file
load_dotenv()

class Config:
    """Central configuration class for the producer."""
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
    KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")
    KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "saas.events.raw")
    KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "saas.events.dlq")

    # Schema Registry Configuration
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
    SCHEMA_REGISTRY_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
    SCHEMA_REGISTRY_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")

    # App Configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls):
        """Validate that all required environment variables are set."""
        required_vars = [
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_API_KEY",
            "KAFKA_API_SECRET",
            "SCHEMA_REGISTRY_URL",
            "SCHEMA_REGISTRY_API_KEY",
            "SCHEMA_REGISTRY_API_SECRET"
        ]
        missing = [var for var in required_vars if not getattr(cls, var)]
        if missing:
            logger.error(f"Missing required environment variables: {', '.join(missing)}")
            raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")
        
        logger.info("Configuration validated successfully.")

# Initialize logger
logger.remove()
logger.add(lambda msg: print(msg), level=Config.LOG_LEVEL)
