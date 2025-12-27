"""
Script MANUAL para atualizar estatísticas de vídeos (views, likes, comments)
Permite escolher qual slot executar manualmente
Versão para execução manual - usuário escolhe o slot
"""
import sys
import os
from datetime import datetime
from typing import List, Optional
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


def get_channels_for_slot(all_channels: List[Channel], slot: int, total_slots: int = 12) -> List[Channel]:
    """
    Obtém canais de um slot específico (função legada para compatibilidade)
    
    Args:
        all_channels: Lista de todos os canais
        slot: Número do slot (0-11)
        total_slots: Número total de slots (padrão 12)
    
    Returns:
        Lista de canais do slot especificado
    """
    if not all_channels:
        return []
    
    if slot < 0 or slot >= total_slots:
        return []
    
    # Calcula quantos canais processar por slot
    channels_per_slot = (len(all_channels) + total_slots - 1) // total_slots  # Arredonda para cima
    
    # Calcula índice inicial e final
    start_idx = slot * channels_per_slot
    end_idx = min(start_idx + channels_per_slot, len(all_channels))
    
    channels_to_process = all_channels[start_idx:end_idx]
    
    return channels_to_process


def display_lotes_info(all_channels: List[Channel]):
    """
    Exibe informações sobre os lotes disponíveis (segmento + slot)
    
    Args:
        all_channels: Lista de todos os canais
    """
    # Separa canais por segmento
    fitness_channels = [ch for ch in all_channels if (ch.segment or '').strip().lower() == 'fitness']
    podcast_channels = [ch for ch in all_channels if (ch.segment or '').strip().lower() == 'podcast']
    
    total_slots = 5
    slot_hours = [1, 3, 5, 7, 9]
    
    log("=" * 60)
    log("INFORMAÇÕES DOS LOTES", "INFO")
    log("=" * 60)
    log(f"Total de canais: {len(all_channels)}")
    log(f"  - Fitness: {len(fitness_channels)} canais")
    log(f"  - Podcast: {len(podcast_channels)} canais")
    log()
    
    log("LOTES FITNESS (Dias pares):")
    for slot in range(total_slots):
        channels = get_channels_by_segment_and_slot(all_channels, 'fitness', slot, total_slots)
        log(f"  Lote {slot+1} ({slot_hours[slot]:02d}:00 BRT): {len(channels)} canais")
    log()
    
    log("LOTES PODCAST (Dias ímpares):")
    for slot in range(total_slots):
        channels = get_channels_by_segment_and_slot(all_channels, 'podcast', slot, total_slots)
        log(f"  Lote {slot+1} ({slot_hours[slot]:02d}:00 BRT): {len(channels)} canais")
    
    log("=" * 60)




def run_update_videos_stats_manual(channel_ids: Optional[List[str]] = None, segment: Optional[str] = None, slot: Optional[int] = None):
    """
    Executa atualização MANUAL de estatísticas de vídeos (views, likes, comments)
    Varre todos os vídeos dos canais e atualiza apenas os que têm mudanças
    VERSÃO MANUAL: Usuário escolhe qual lote executar (segmento + slot)
    
    Args:
        channel_ids: Lista de IDs de canais para atualizar. Se None, permite escolher lotes.
        segment: Segmento ('fitness' ou 'podcast'). Se None, determina pelo dia ou solicita.
        slot: Número do slot (0-4). Se None, processa todos os slots do segmento.
    """
    try:
        log("=" * 60)
        log("MODO MANUAL: Atualização por slot selecionado", "INFO")
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
            
            # Extrai IDs dos canais
            channel_ids_list = [c.channel_id for c in channels]
        else:
            # Busca todos os canais
            all_channels = supabase_client.get_channels()
            
            if not all_channels:
                log("Nenhum canal encontrado", "ERROR")
                return False
            
            # Determina segmento
            if segment is None:
                # Se não foi especificado, verifica variável de ambiente ou determina pelo dia
                segment_env = os.getenv("SEGMENT", "").strip().lower()
                if segment_env in ['fitness', 'podcast']:
                    segment = segment_env
                else:
                    # Determina pelo dia atual
                    current_day = datetime.now().day
                    is_even_day = (current_day % 2) == 0
                    segment = 'fitness' if is_even_day else 'podcast'
                    log(f"Dia {current_day} ({'par' if is_even_day else 'ímpar'}) - Usando segmento: {segment.upper()}", "INFO")
            else:
                segment = segment.lower()
            
            if segment not in ['fitness', 'podcast']:
                log(f"Segmento inválido: {segment}. Use 'fitness' ou 'podcast'", "ERROR")
                return False
            
            # Determina slot(s)
            if slot is None:
                slot_env = os.getenv("SLOT", "").strip()
                if slot_env:
                    try:
                        slot = int(slot_env) - 1  # Converte para 0-indexed
                        if slot < 0 or slot >= 5:
                            log(f"Slot inválido: {slot_env}. Use 1-5", "ERROR")
                            return False
                        selected_slots = {slot}
                    except ValueError:
                        log(f"Slot inválido: {slot_env}", "ERROR")
                        return False
                else:
                    # Processa todos os slots do segmento
                    selected_slots = set(range(5))
                    log(f"Processando todos os 5 slots do segmento {segment.upper()}", "INFO")
            else:
                if slot < 0 or slot >= 5:
                    log(f"Slot inválido: {slot}. Use 0-4", "ERROR")
                    return False
                selected_slots = {slot}
            
            # Coleta canais dos lotes selecionados
            channels = []
            slot_hours = [1, 3, 5, 7, 9]
            for s in sorted(selected_slots):
                slot_channels = get_channels_by_segment_and_slot(all_channels, segment, s, total_slots=5)
                channels.extend(slot_channels)
                log(f"Lote {s+1} ({slot_hours[s]:02d}:00 BRT) - {segment.upper()}: {len(slot_channels)} canais")
            
            if not channels:
                log(f"Nenhum canal encontrado nos lotes selecionados (segmento: {segment}, slots: {sorted(selected_slots)})", "ERROR")
                return False
            
            log(f"Total de canais a processar: {len(channels)} (segmento: {segment.upper()}, lotes: {', '.join([str(s+1) for s in sorted(selected_slots)])})")
            
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
    segment_env = os.getenv("SEGMENT", "").strip().lower()
    slot_env = os.getenv("SLOT", "").strip()
    
    channel_ids = None
    segment = None
    slot = None
    
    if channel_ids_env:
        # Suporta múltiplos channel_ids separados por vírgula
        channel_ids = [cid.strip() for cid in channel_ids_env.split(',') if cid.strip()]
        log(f"Modo MANUAL: Atualização de {len(channel_ids)} canal(is) específico(s)", "INFO")
    else:
        # Processa por segmento e slot
        if segment_env in ['fitness', 'podcast']:
            segment = segment_env
            log(f"Modo MANUAL: Segmento {segment.upper()}", "INFO")
        else:
            log("Modo MANUAL: Segmento será determinado pelo dia (par=fitness, ímpar=podcast)", "INFO")
        
        if slot_env:
            try:
                slot = int(slot_env) - 1  # Converte para 0-indexed
                if 0 <= slot < 5:
                    log(f"Modo MANUAL: Lote {slot+1} selecionado", "INFO")
                else:
                    log(f"Lote inválido: {slot_env}. Use 1-5", "ERROR")
                    sys.exit(1)
            except ValueError:
                log(f"Lote inválido: {slot_env}. Use um número de 1 a 5", "ERROR")
                sys.exit(1)
        else:
            log("Modo MANUAL: Processando todos os lotes do segmento", "INFO")
    
    success = run_update_videos_stats_manual(channel_ids=channel_ids, segment=segment, slot=slot)
    sys.exit(0 if success else 1)

