"""
Funções auxiliares
"""
from datetime import datetime, timedelta
import re
from typing import Optional, Tuple


def parse_iso8601_duration(duration: str) -> int:
    """
    Converte duração ISO 8601 (ex: PT4M13S) para segundos
    """
    if not duration or not duration.startswith('PT'):
        return 0
    
    # Remove o prefixo PT
    duration = duration[2:]
    
    # Extrai horas, minutos e segundos
    hours = 0
    minutes = 0
    seconds = 0
    
    # Padrão para horas
    hour_match = re.search(r'(\d+)H', duration)
    if hour_match:
        hours = int(hour_match.group(1))
    
    # Padrão para minutos
    minute_match = re.search(r'(\d+)M', duration)
    if minute_match:
        minutes = int(minute_match.group(1))
    
    # Padrão para segundos
    second_match = re.search(r'(\d+)S', duration)
    if second_match:
        seconds = int(second_match.group(1))
    
    return hours * 3600 + minutes * 60 + seconds


def detect_short(duration: str, title: str, description: str = "") -> Tuple[str, bool, bool]:
    """
    Detecta se um vídeo é Short baseado apenas na duração e identifica vídeos inválidos
    
    Retorna: (format, is_short, is_invalid)
    - format: "9:16" para Short, "16:9" para vídeo normal
    - is_short: True se for Short (duração < 3 minutos), False caso contrário
    - is_invalid: True se duração for 0 segundos (vídeo inválido), False caso contrário
    """
    # Converte duração para segundos
    duration_seconds = parse_iso8601_duration(duration)
    
    # Vídeo inválido se duração for 0 segundos
    is_invalid = duration_seconds == 0
    
    # É Short se duração for menor que 3 minutos (180 segundos) E não for inválido
    is_short = duration_seconds < 180 and not is_invalid
    
    format_type = "9:16" if is_short else "16:9"
    
    return format_type, is_short, is_invalid


def parse_datetime(date_str: Optional[str]) -> Optional[datetime]:
    """Converte string de data para datetime (sempre com timezone UTC)"""
    if not date_str:
        return None
    
    try:
        # Formato ISO 8601 do YouTube
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        # Garante que tem timezone (UTC se não tiver)
        if dt.tzinfo is None:
            from datetime import timezone
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except:
        try:
            # Tenta outros formatos comuns
            dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            # Adiciona timezone UTC se não tiver
            if dt.tzinfo is None:
                from datetime import timezone
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except:
            return None


def format_datetime(dt: Optional[datetime]) -> Optional[str]:
    """Converte datetime para string ISO 8601"""
    if not dt:
        return None
    return dt.isoformat()


def get_date_before(date_str: Optional[str], days: int = 1) -> Optional[str]:
    """Retorna data N dias antes da data fornecida"""
    if not date_str:
        return None
    
    dt = parse_datetime(date_str)
    if not dt:
        return None
    
    before_dt = dt - timedelta(days=days)
    return format_datetime(before_dt)


def get_date_after(date_str: Optional[str], days: int = 1) -> Optional[str]:
    """Retorna data N dias depois da data fornecida"""
    if not date_str:
        return None
    
    dt = parse_datetime(date_str)
    if not dt:
        return None
    
    after_dt = dt + timedelta(days=days)
    return format_datetime(after_dt)


def is_afternoon_time(hour: int) -> bool:
    """Verifica se o horário é da tarde (12:00 - 18:00)"""
    return 12 <= hour < 18


def is_night_time(hour: int) -> bool:
    """Verifica se o horário é da noite/madrugada (18:00 - 12:00)"""
    return hour >= 18 or hour < 12

