"""
Configurações do sistema de extração de vídeos do YouTube
"""
import os
import json
from pathlib import Path

# Diretório base do projeto
BASE_DIR = Path(__file__).parent

# Configurações do MySQL
# Lê de variáveis de ambiente (obrigatório para produção)
# Configure as variáveis de ambiente: MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

if not all([MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE]):
    raise ValueError(
        "Configurações MySQL não encontradas! "
        "Configure as variáveis de ambiente: MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE"
    )

# Chave inicial da API do YouTube
# Lê de variáveis de ambiente (obrigatório)
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
if not YOUTUBE_API_KEY:
    raise ValueError(
        "YOUTUBE_API_KEY não configurada! "
        "Configure a variável de ambiente YOUTUBE_API_KEY"
    )

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

