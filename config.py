import dotenv

dotenv.load_dotenv()


class Config:
    OPENAI_API_KEY: str = dotenv.get_key(dotenv.find_dotenv(), "OPENAI_API_KEY")
    MODEL: str = dotenv.get_key(dotenv.find_dotenv(), "MODEL")
    BASE_URL: str = dotenv.get_key(dotenv.find_dotenv(), "BASE_URL")

    COMPLETION_CLIENT_CONFIG = {
        "api_key": OPENAI_API_KEY,
        "model": MODEL,
        "base_url": BASE_URL,
    }

    @classmethod
    def validate(cls):
        missing = []
        for key, value in cls.COMPLETION_CLIENT_CONFIG.items():
            if value is None:
                missing.append(key)
        if missing:
            raise ValueError(f"Missing required configuration(s): {', '.join(missing)}")
