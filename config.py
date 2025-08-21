import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # OpenAI Configuration
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
    
    # Telegram Configuration
    ACCOUNTS_FILE = os.getenv('ACCOUNTS_FILE', 'telegram_accounts.json')
    
    # AWS S3 Configuration (for bookmark feature)
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'telegram-scraper-bucket')
    
    # API Configuration
    API_HOST = os.getenv('API_HOST', '0.0.0.0')
    API_PORT = int(os.getenv('API_PORT', 5000))
    
    # Rate Limiting
    DEFAULT_RATE_LIMIT_SECONDS = 3600
    MAX_RETRIES = 3
    
    # Message Processing
    MESSAGES_LIMIT = 100
    TOP_USERS_LIMIT = 50
    
    # Supported Languages
    SUPPORTED_LANGUAGES = {
        "english": {"english": "English", "native": "English"},
        "hindi": {"english": "Hindi", "native": "हिन्दी"},
        "bengali": {"english": "Bengali", "native": "বাংলা"},
        "telugu": {"english": "Telugu", "native": "తెలుగు"},
        "marathi": {"english": "Marathi", "native": "मराठी"},
        "tamil": {"english": "Tamil", "native": "தமிழ்"},
        "gujarati": {"english": "Gujarati", "native": "ગુજરાતી"},
        "urdu": {"english": "Urdu", "native": "اردو"},
        "kannada": {"english": "Kannada", "native": "ಕನ್ನಡ"},
        "odia": {"english": "Odia", "native": "ଓଡ଼ିଆ"},
        "malayalam": {"english": "Malayalam", "native": "മലയാളം"},
        "punjabi": {"english": "Punjabi", "native": "ਪੰਜਾਬੀ"},
        "assamese": {"english": "Assamese", "native": "অসমীয়া"},
        "maithili": {"english": "Maithili", "native": "मैथिली"},
        "santali": {"english": "Santali", "native": "ᱥᱟᱱᱛᱟᱲᱤ"},
        "konkani": {"english": "Konkani", "native": "कोंकणी"},
        "sindhi": {"english": "Sindhi", "native": "سنڌي"},
        "dogri": {"english": "Dogri", "native": "डोगरी"},
        "kashmiri": {"english": "Kashmiri", "native": "کٲشُر"},
        "sanskrit": {"english": "Sanskrit", "native": "संस्कृतम्"},
        "nepali": {"english": "Nepali", "native": "नेपाली"}
    }

config = Config()