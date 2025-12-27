"""
Script para atualizar estatísticas de vídeos (views, likes, comments)
Executa de 2 em 2 horas, varrendo todos os vídeos e atualizando apenas os que têm mudanças
Divide canais por hora para distribuir carga
"""
import sys
import os
from datetime import datetime
from typing import List, Optional, Dict
import config
from api_key_manager import APIKeyManager
from supabase_client import SupabaseClient
from youtube_updater import YouTubeUpdater
from models import Channel


def log(message: str, level: str = "INFO"):
    """Adiciona mensagem aos logs"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = {
        "INFO": "[INFO]",
        "SUCCESS": "[✓]",
        "ERROR": "[✗]",
        "WARNING": "[!]"
    }.get(level, "[INFO]")
    
    print(f"{timestamp} {prefix} {message}")
    sys.stdout.flush()


def get_channels_by_segment_and_slot(all_channels: List[Channel], segment: str, slot: int, total_slots: int = 5) -> List[Channel]:
    """
    Obtém canais de um segmento específico e slot específico
    
    Args:
        all_channels: Lista de todos os canais
        segment: Segmento ('fitness' ou 'podcast')
        slot: Número do slot (0-4, onde 0=1h, 1=3h, 2=5h, 3=7h, 4=9h BRT)
        total_slots: Número total de slots (padrão 5)
    
    Returns:
        Lista de canais do segmento e slot especificados
    """
    if not all_channels:
        return []
    
    # Filtra canais por segmento
    segment_channels = []
    for ch in all_channels:
        ch_segment = (ch.segment or '').strip().lower()
        if ch_segment == segment.lower():
            segment_channels.append(ch)
    
    if not segment_channels:
        return []
    
    # Ordena canais por channel_id para garantir distribuição estável e determinística
    # Isso garante que novos canais sejam distribuídos de forma consistente
    segment_channels.sort(key=lambda ch: ch.channel_id)
    
    # Distribui canais do segmento entre os slots
    channels_per_slot = (len(segment_channels) + total_slots - 1) // total_slots  # Arredonda para cima
    
    # Calcula índice inicial e final para o slot
    start_idx = slot * channels_per_slot
    end_idx = min(start_idx + channels_per_slot, len(segment_channels))
    
    channels_to_process = segment_channels[start_idx:end_idx]
    
    return channels_to_process


def get_channels_for_current_hour(all_channels: List[Channel]) -> List[Channel]:
    """
    Obtém canais para processar na hora atual baseado em:
    - Dia par/ímpar (determina segmento: fitness ou podcast)
    - Hora atual (determina slot: 1h, 3h, 5h, 7h, 9h BRT)
    
    Args:
        all_channels: Lista de todos os canais
    
    Returns:
        Lista de canais para processar na hora atual
    """
    if not all_channels:
        return []
    
    # Obtém dia atual (1-31)
    current_day = datetime.now().day
    is_even_day = (current_day % 2) == 0
    
    # Determina segmento baseado no dia
    # Dias pares = Fitness, Dias ímpares = Podcast
    segment = 'fitness' if is_even_day else 'podcast'
    
    # Obtém a hora atual (0-23) em UTC
    current_hour_utc = datetime.utcnow().hour
    
    # Mapeia hora UTC para slot BRT
    # 1h BRT = 4h UTC, 3h BRT = 6h UTC, 5h BRT = 8h UTC, 7h BRT = 10h UTC, 9h BRT = 12h UTC
    hour_to_slot = {
        4: 0,   # 01:00 BRT
        6: 1,   # 03:00 BRT
        8: 2,   # 05:00 BRT
        10: 3,  # 07:00 BRT
        12: 4   # 09:00 BRT
    }
    
    slot = hour_to_slot.get(current_hour_utc, -1)
    
    if slot == -1:
        log(f"Hora atual ({current_hour_utc:02d}:00 UTC) não corresponde a nenhum slot de execução", "WARNING")
        return []
    
    # Obtém canais do segmento e slot
    channels = get_channels_by_segment_and_slot(all_channels, segment, slot, total_slots=5)
    
    slot_hours_brt = [1, 3, 5, 7, 9]
    log(f"Dia {current_day} ({'par' if is_even_day else 'ímpar'}) - Segmento: {segment.upper()}")
    log(f"Slot {slot+1}/5 - Hora {slot_hours_brt[slot]:02d}:00 BRT ({current_hour_utc:02d}:00 UTC)")
    log(f"Canais neste lote: {len(channels)}")
    
    return channels


def run_update_videos_stats(channel_ids: Optional[List[str]] = None):
    """
    Executa atualização de estatísticas de vídeos (views, likes, comments)
    Varre todos os vídeos dos canais e atualiza apenas os que têm mudanças
    
    Args:
        channel_ids: Lista de IDs de canais para atualizar. Se None, divide por hora.
    """
    try:
        # Carrega configurações de variáveis de ambiente (GitHub Secrets)
        if os.getenv('SUPABASE_URL'):
            config.SUPABASE_URL = os.getenv('SUPABASE_URL')
        if os.getenv('SUPABASE_KEY'):
            config.SUPABASE_KEY = os.getenv('SUPABASE_KEY')
        
        # Carrega chaves de API de variáveis de ambiente
        api_keys = []
        if os.getenv('YOUTUBE_API_KEY'):
            api_keys.append(os.getenv('YOUTUBE_API_KEY'))
        
        # Suporta múltiplas chaves separadas por vírgula
        if os.getenv('YOUTUBE_API_KEYS'):
            additional_keys = os.getenv('YOUTUBE_API_KEYS').split(',')
            api_keys.extend([k.strip() for k in additional_keys if k.strip()])
        
        if not api_keys:
            log("Nenhuma chave de API configurada nas variáveis de ambiente!", "ERROR")
            return False
        
        # Inicializa componentes
        api_key_manager = APIKeyManager()
        if api_keys:
            api_key_manager.keys = api_keys
            api_key_manager.current_key_index = 0
        
        supabase_client = SupabaseClient()
        youtube_updater = YouTubeUpdater(api_key_manager, supabase_client)
        
        # Verifica se há chaves disponíveis
        if not api_key_manager.has_available_keys():
            log("Nenhuma chave de API disponível!", "ERROR")
            return False
        
        # Busca canais
        if channel_ids:
            log(f"Iniciando atualização de vídeos de {len(channel_ids)} canal(is) específico(s)")
            channels = []
            for channel_id in channel_ids:
                channel = supabase_client.get_channel_by_id(channel_id.strip())
                if channel:
                    channels.append(channel)
                else:
                    log(f"Canal não encontrado: {channel_id}", "WARNING")
            
            if not channels:
                log("Nenhum canal válido encontrado", "ERROR")
                return False
        else:
            log("Iniciando atualização de vídeos - modo automático (segmento + slot por dia/hora)")
            all_channels = supabase_client.get_channels()
            
            if not all_channels:
                log("Nenhum canal encontrado", "ERROR")
                return False
            
            # Obtém canais baseado em dia par/ímpar e hora atual
            channels = get_channels_for_current_hour(all_channels)
            
            if not channels:
                log("Nenhum canal para processar neste horário", "INFO")
                return True
        
        log(f"Encontrados {len(channels)} canal(is) para processar")
        
        # Extrai IDs dos canais
        channel_ids_list = [c.channel_id for c in channels]
        
        # Atualiza vídeos de todos os canais selecionados
        total_stats = youtube_updater.update_all_channels_videos(channel_ids_list, log_callback=log)
        
        # Resumo final
        log("=" * 60)
        log(f"Atualização completa concluída!", "SUCCESS")
        log(f"Total de vídeos processados: {total_stats['total']}", "INFO")
        log(f"Atualizados: {total_stats['updated']}", "SUCCESS")
        log(f"Sem mudanças: {total_stats['unchanged']}", "INFO")
        log(f"Erros: {total_stats['errors']}", "ERROR" if total_stats['errors'] > 0 else "INFO")
        log(f"Não encontrados na API: {total_stats['not_found']}", "WARNING" if total_stats['not_found'] > 0 else "INFO")
        
        # Exibe informações de quota
        quota_info = youtube_updater.get_quota_info()
        log(f"Quota da API: {quota_info['used']}/{quota_info['limit']} usada ({quota_info['percentage_used']:.1f}%)", "INFO")
        log(f"Quota restante: {quota_info['remaining']} unidades", "INFO")
        breakdown = quota_info['breakdown']
        if breakdown['videos_list'] > 0:
            log(f"Detalhamento: videos.list={breakdown['videos_list']}", "INFO")
        
        return True
        
    except Exception as e:
        log(f"Erro na atualização: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Verifica se foi especificado canais via variável de ambiente
    channel_ids_env = os.getenv("CHANNEL_IDS")
    channel_ids = None
    
    if channel_ids_env:
        # Suporta múltiplos channel_ids separados por vírgula
        channel_ids = [cid.strip() for cid in channel_ids_env.split(',') if cid.strip()]
        log(f"Modo: Atualização de {len(channel_ids)} canal(is) específico(s)", "INFO")
    else:
        log("Modo: Atualização com divisão automática por hora", "INFO")
    
    success = run_update_videos_stats(channel_ids=channel_ids)
    sys.exit(0 if success else 1)

