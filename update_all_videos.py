"""
Script para atualizar vídeos de canais específicos
Valida duração, canal_id e atualiza informações se houver diferenças
Suporta processamento paralelo com 3 workers simultâneos
"""
import sys
import os
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
import config
from api_key_manager import APIKeyManager
from supabase_client import SupabaseClient
from youtube_extractor import YouTubeExtractor
from utils import parse_datetime, format_datetime
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


def videos_differ(existing_video, new_video) -> bool:
    """Verifica se há diferenças entre vídeo existente e novo"""
    # Compara campos principais
    if existing_video.title != new_video.title:
        return True
    if existing_video.views != new_video.views:
        return True
    if existing_video.likes != new_video.likes:
        return True
    if existing_video.comments != new_video.comments:
        return True
    if existing_video.duration != new_video.duration:
        return True
    if existing_video.channel_id != new_video.channel_id:
        return True
    
    # Compara tags (normaliza para lista)
    existing_tags = existing_video.tags
    if isinstance(existing_tags, str):
        try:
            existing_tags = json.loads(existing_tags)
        except:
            existing_tags = []
    if not isinstance(existing_tags, list):
        existing_tags = []
    
    new_tags = new_video.tags
    if isinstance(new_tags, str):
        try:
            new_tags = json.loads(new_tags)
        except:
            new_tags = []
    if not isinstance(new_tags, list):
        new_tags = []
    
    if sorted(existing_tags) != sorted(new_tags):
        return True
    
    return False


def validate_video_belongs_to_channel(video_channel_id: str, expected_channel_id: str) -> bool:
    """Valida se o vídeo realmente pertence ao canal esperado"""
    return video_channel_id == expected_channel_id


def process_single_channel(
    channel: Channel,
    youtube_extractor: YouTubeExtractor,
    supabase_client: SupabaseClient,
    channel_index: int,
    total_channels: int
) -> dict:
    """Processa um único canal e retorna estatísticas"""
    channel_stats = {
        'channel_id': channel.channel_id,
        'channel_name': channel.name,
        'new': 0,
        'updated': 0,
        'skipped': 0,
        'errors': 0,
        'invalid_channel': 0
    }
    
    try:
        log(f"[{channel_index}/{total_channels}] Processando: {channel.name} (ID: {channel.channel_id})")
        
        # Obtém playlist de uploads
        playlist_id = youtube_extractor.get_upload_playlist_id(channel.channel_id)
        if not playlist_id:
            log(f"  Erro: Não foi possível obter playlist do canal", "ERROR")
            channel_stats['errors'] = 1
            return channel_stats
        
        # Busca TODOS os vídeos do canal
        log(f"  Buscando todos os vídeos do canal...")
        videos_data = youtube_extractor.get_all_videos_from_playlist(playlist_id, start_date=None)
        
        if not videos_data:
            log(f"  Nenhum vídeo encontrado no canal")
            return channel_stats
        
        log(f"  Encontrados {len(videos_data)} vídeos no canal")
        
        # Processa vídeos
        videos = youtube_extractor.process_videos(videos_data, channel.channel_id)
        
        oldest_date = None
        newest_date = None
        
        for j, video in enumerate(videos):
            if j % 50 == 0 and j > 0:
                log(f"  Processando vídeo {j+1}/{len(videos)}...")
            
            # Valida se o vídeo pertence ao canal correto
            if not validate_video_belongs_to_channel(video.channel_id, channel.channel_id):
                log(f"  [AVISO] Vídeo {video.video_id} não pertence ao canal {channel.channel_id} (pertence a {video.channel_id})", "WARNING")
                channel_stats['invalid_channel'] += 1
                continue
            
            # Busca vídeo existente
            existing_video = supabase_client.get_video_by_id(video.video_id)
            
            if existing_video:
                # Valida canal_id do vídeo existente
                if existing_video.channel_id != channel.channel_id:
                    log(f"  [AVISO] Vídeo {video.video_id} existe mas pertence a outro canal (DB: {existing_video.channel_id}, Esperado: {channel.channel_id})", "WARNING")
                    channel_stats['invalid_channel'] += 1
                    continue
                
                # Verifica se há diferenças
                if videos_differ(existing_video, video):
                    # Atualiza vídeo
                    if supabase_client.update_video(video):
                        channel_stats['updated'] += 1
                        log(f"  [ATUALIZADO] {video.video_id}: {video.title[:50]}...")
                    else:
                        channel_stats['errors'] += 1
                        log(f"  [ERRO] Falha ao atualizar vídeo {video.video_id}", "ERROR")
                else:
                    channel_stats['skipped'] += 1
            else:
                # Insere vídeo novo
                if supabase_client.insert_video(video):
                    channel_stats['new'] += 1
                    log(f"  [NOVO] {video.video_id}: {video.title[:50]}...")
                else:
                    channel_stats['errors'] += 1
                    log(f"  [ERRO] Falha ao inserir vídeo {video.video_id}", "ERROR")
            
            # Atualiza datas para o canal
            if video.published_at:
                pub_date = parse_datetime(video.published_at)
                if pub_date:
                    if not oldest_date or pub_date < oldest_date:
                        oldest_date = pub_date
                    if not newest_date or pub_date > newest_date:
                        newest_date = pub_date
        
        # Atualiza datas do canal
        if oldest_date and newest_date:
            supabase_client.update_channel_dates(
                channel.channel_id,
                oldest_date=format_datetime(oldest_date),
                newest_date=format_datetime(newest_date)
            )
            log(f"  Datas do canal atualizadas: {format_datetime(oldest_date)} até {format_datetime(newest_date)}")
        
        log(f"  [{channel_index}/{total_channels}] ✓ {channel.name}: {channel_stats['new']} novos, {channel_stats['updated']} atualizados, {channel_stats['skipped']} sem mudanças", "SUCCESS")
        
    except Exception as e:
        log(f"  [{channel_index}/{total_channels}] ✗ Erro ao processar canal {channel.name}: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        channel_stats['errors'] += 1
    
    return channel_stats


def run_full_update(channel_ids: Optional[List[str]] = None):
    """
    Executa atualização de vídeos de canais específicos com processamento paralelo
    
    Args:
        channel_ids: Lista de IDs de canais para atualizar. Se None, busca todos os canais.
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
        youtube_extractor = YouTubeExtractor(api_key_manager)
        
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
            log("Iniciando atualização de vídeos de todos os canais")
            channels = supabase_client.get_channels()
        
        log(f"Encontrados {len(channels)} canal(is) para processar")
        log(f"Processamento paralelo: 3 workers simultâneos")
        
        if not channels:
            log("Nenhum canal encontrado", "ERROR")
            return False
        
        total_videos_processed = 0
        total_new = 0
        total_updated = 0
        total_skipped = 0
        total_errors = 0
        total_invalid_channel = 0
        
        # Processa canais em paralelo com 3 workers
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Submete todas as tarefas
            future_to_channel = {
                executor.submit(
                    process_single_channel,
                    channel,
                    youtube_extractor,
                    supabase_client,
                    i + 1,
                    len(channels)
                ): channel
                for i, channel in enumerate(channels)
            }
            
            # Processa resultados conforme completam
            for future in as_completed(future_to_channel):
                channel = future_to_channel[future]
                try:
                    stats = future.result()
                    total_new += stats['new']
                    total_updated += stats['updated']
                    total_skipped += stats['skipped']
                    total_errors += stats['errors']
                    total_invalid_channel += stats['invalid_channel']
                    total_videos_processed += stats['new'] + stats['updated']
                except Exception as e:
                    log(f"Erro ao processar resultado do canal {channel.name}: {e}", "ERROR")
                    total_errors += 1
        
        log("="*60, "SUCCESS")
        log(f"Atualização completa concluída!", "SUCCESS")
        log(f"Total de vídeos processados: {total_videos_processed}", "INFO")
        log(f"Novos: {total_new}", "INFO")
        log(f"Atualizados: {total_updated}", "INFO")
        log(f"Sem mudanças: {total_skipped}", "INFO")
        log(f"Erros: {total_errors}", "ERROR" if total_errors > 0 else "INFO")
        log(f"Vídeos com canal_id inválido: {total_invalid_channel}", "WARNING" if total_invalid_channel > 0 else "INFO")
        
        # Exibe informações de quota
        quota_info = youtube_extractor.get_quota_info()
        log(f"Quota da API: {quota_info['used']}/{quota_info['limit']} usada ({quota_info['percentage_used']:.1f}%)", "INFO")
        log(f"Quota restante: {quota_info['remaining']} unidades", "INFO")
        breakdown = quota_info['breakdown']
        if breakdown['channels_list'] > 0 or breakdown['playlist_items'] > 0 or breakdown['videos_list'] > 0:
            log(f"Detalhamento: channels.list={breakdown['channels_list']}, playlistItems.list={breakdown['playlist_items']}, videos.list={breakdown['videos_list']}", "INFO")
        
        return True
        
    except Exception as e:
        log(f"Erro na atualização completa: {e}", "ERROR")
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
        log("Modo: Atualização de todos os canais", "INFO")
    
    success = run_full_update(channel_ids=channel_ids)
    sys.exit(0 if success else 1)

