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


def get_channels_for_current_hour(all_channels: List[Channel], total_hours: int = 12) -> List[Channel]:
    """
    Divide canais por hora para distribuir carga ao longo do dia
    
    Args:
        all_channels: Lista de todos os canais
        total_hours: Número de horas no ciclo (12 horas = execução a cada 2 horas)
    
    Returns:
        Lista de canais para processar na hora atual
    """
    if not all_channels:
        return []
    
    # Obtém a hora atual (0-23)
    current_hour = datetime.now().hour
    
    # Calcula qual "slot" de 2 horas estamos (0-11)
    # Executa a cada 2 horas, então temos 12 slots por dia
    slot = (current_hour // 2) % total_hours
    
    # Calcula quantos canais processar por slot
    channels_per_slot = (len(all_channels) + total_hours - 1) // total_hours  # Arredonda para cima
    
    # Calcula índice inicial e final
    start_idx = slot * channels_per_slot
    end_idx = min(start_idx + channels_per_slot, len(all_channels))
    
    channels_to_process = all_channels[start_idx:end_idx]
    
    log(f"Slot atual: {slot}/{total_hours-1} (hora {current_hour:02d}:00)")
    log(f"Processando canais {start_idx+1} a {end_idx} de {len(all_channels)} total")
    log(f"Canais neste slot: {len(channels_to_process)}")
    
    return channels_to_process


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
            log("Iniciando atualização de vídeos - modo divisão por hora")
            all_channels = supabase_client.get_channels()
            
            if not all_channels:
                log("Nenhum canal encontrado", "ERROR")
                return False
            
            # Divide canais por hora (execução a cada 2 horas = 12 slots por dia)
            channels = get_channels_for_current_hour(all_channels, total_hours=12)
            
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

