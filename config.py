"""
Configurações do sistema de extração de vídeos do YouTube
"""
import os
import json
from pathlib import Path

# Diretório base do projeto
BASE_DIR = Path(__file__).parent

# Configurações do Supabase
# Lê de variáveis de ambiente se disponível (para GitHub Actions), senão usa valores padrão
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://rmhozuzxcytguvluksih.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJtaG96dXp4Y3l0Z3V2bHVrc2loIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI2NDc4NjIsImV4cCI6MjA3ODIyMzg2Mn0.sOOFm246T0sVBVNOOYmyDFmvGKzet2X5rJvwp0o1UAU")

# Chave inicial da API do YouTube
# Lê de variáveis de ambiente se disponível (para GitHub Actions), senão usa valor padrão
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "AIzaSyCl5dHCtinYrqz5fv_pItVrIWzXLozWVtQ")

# Arquivo de configuração de chaves
API_KEYS_FILE = BASE_DIR / "api_keys.json"
SCHEDULE_CONFIG_FILE = BASE_DIR / "schedule_config.json"
CACHE_FILE = BASE_DIR / "cache.json"
CHECKPOINT_FILE = BASE_DIR / "checkpoint.json"

# Limites e configurações
MAX_VIDEOS_PER_EXECUTION = 50
FETCH_ALL_VIDEOS_AT_ONCE = True  # Se True, busca todos os vídeos de uma vez (pode consumir muita quota)
QUOTA_DAILY_LIMIT = 10000
QUOTA_WARNING_THRESHOLD = 1000
QUOTA_STOP_THRESHOLD = 100
RETRY_MAX_ATTEMPTS = 3
RETRY_DELAY_BASE = 1  # segundos
REQUEST_DELAY = 0.5  # segundos entre requisições
CHANNEL_DELAY = 0.5  # segundos entre canais

# Configurações de atualização de canais (update_channels.py)
MAX_CONCURRENT_CHANNELS = 2  # Número máximo de canais processados em paralelo (reduzido para evitar problemas de memória)
CHANNEL_TIMEOUT = 30  # Timeout em segundos para processar um canal
BATCH_SIZE = 10  # Tamanho do lote de canais processados antes de salvar checkpoint (reduzido para economizar memória)
CHECKPOINT_INTERVAL = 10  # Salvar checkpoint a cada N canais processados
RATE_LIMIT_DELAY = 0.5  # Delay entre requisições para respeitar rate limit (aumentado)

def load_api_keys():
    """Carrega lista de chaves de API do arquivo"""
    if API_KEYS_FILE.exists():
        try:
            with open(API_KEYS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('keys', [YOUTUBE_API_KEY])
        except Exception:
            return [YOUTUBE_API_KEY]
    return [YOUTUBE_API_KEY]

def save_api_keys(keys):
    """Salva lista de chaves de API no arquivo"""
    try:
        with open(API_KEYS_FILE, 'w', encoding='utf-8') as f:
            json.dump({'keys': keys}, f, indent=2)
    except Exception as e:
        print(f"Erro ao salvar chaves: {e}")

def load_schedule_config():
    """Carrega configuração de agendamento"""
    if SCHEDULE_CONFIG_FILE.exists():
        try:
            with open(SCHEDULE_CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {'enabled': False, 'times': []}
    return {'enabled': False, 'times': []}

def save_schedule_config(config):
    """Salva configuração de agendamento"""
    try:
        with open(SCHEDULE_CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        print(f"Erro ao salvar configuração: {e}")

def load_cache():
    """Carrega cache local"""
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(cache):
    """Salva cache local"""
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"Erro ao salvar cache: {e}")

