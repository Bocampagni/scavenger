from typing import Optional
import dotenv

dotenv.load_dotenv()


class Config:
    OPENAI_API_KEY: str = dotenv.get_key(dotenv.find_dotenv(), "OPENAI_API_KEY")
    MODEL: str = dotenv.get_key(dotenv.find_dotenv(), "MODEL")
    BASE_URL: str = dotenv.get_key(dotenv.find_dotenv(), "BASE_URL")
    GCP_API_KEY: Optional[str] = dotenv.get_key(dotenv.find_dotenv(), "GCP_API_KEY")

    COMPLETION_CLIENT_CONFIG = {
        "api_key": OPENAI_API_KEY,
        "model": MODEL,
        "base_url": BASE_URL,
    }