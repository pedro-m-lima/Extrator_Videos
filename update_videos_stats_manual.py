"""
Script MANUAL para atualizar estatísticas de vídeos (views, likes, comments)
Executa atualização completa de TODOS os canais de uma vez
Versão para execução manual - não divide por hora
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


def run_update_videos_stats_manual(channel_ids: Optional[List[str]] = None):
    """
    Executa atualização MANUAL de estatísticas de vídeos (views, likes, comments)
    Varre todos os vídeos dos canais e atualiza apenas os que têm mudanças
    VERSÃO MANUAL: Processa TODOS os canais de uma vez, sem divisão por hora
    
    Args:
        channel_ids: Lista de IDs de canais para atualizar. Se None, atualiza TODOS os canais.
    """
    try:
        log("=" * 60)
        log("MODO MANUAL: Atualização completa de todos os canais", "INFO")
        log("=" * 60)
        
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
            log(f"Iniciando atualização MANUAL de vídeos de {len(channel_ids)} canal(is) específico(s)")
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
            log("Iniciando atualização MANUAL de vídeos de TODOS os canais")
            log("ATENÇÃO: Esta execução processará TODOS os canais de uma vez!", "WARNING")
            channels = supabase_client.get_channels()
            
            if not channels:
                log("Nenhum canal encontrado", "ERROR")
                return False
        
        log(f"Total de canais a processar: {len(channels)}")
        log(f"Esta execução pode levar bastante tempo dependendo da quantidade de vídeos", "INFO")
        
        # Extrai IDs dos canais
        channel_ids_list = [c.channel_id for c in channels]
        
        # Atualiza vídeos de todos os canais selecionados
        log("=" * 60)
        log("Iniciando processamento...", "INFO")
        log("=" * 60)
        
        total_stats = youtube_updater.update_all_channels_videos(channel_ids_list, log_callback=log)
        
        # Resumo final
        log("=" * 60)
        log("ATUALIZAÇÃO MANUAL CONCLUÍDA!", "SUCCESS")
        log("=" * 60)
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
        log(f"Modo MANUAL: Atualização de {len(channel_ids)} canal(is) específico(s)", "INFO")
    else:
        log("Modo MANUAL: Atualização de TODOS os canais", "INFO")
        log("Para atualizar canais específicos, use: CHANNEL_IDS=id1,id2,id3 python update_videos_stats_manual.py", "INFO")
    
    success = run_update_videos_stats_manual(channel_ids=channel_ids)
    sys.exit(0 if success else 1)

