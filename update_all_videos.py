"""
Script para atualizar todos os vídeos de todos os canais
Valida duração, canal_id e atualiza informações se houver diferenças
"""
import sys
import os
import json
from datetime import datetime
import config
from api_key_manager import APIKeyManager
from supabase_client import SupabaseClient
from youtube_extractor import YouTubeExtractor
from utils import parse_datetime, format_datetime


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


def run_full_update():
    """Executa atualização completa de todos os vídeos de todos os canais"""
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
        
        log("Iniciando atualização completa de todos os vídeos de todos os canais")
        
        # Verifica se há chaves disponíveis
        if not api_key_manager.has_available_keys():
            log("Nenhuma chave de API disponível!", "ERROR")
            return False
        
        # Busca todos os canais
        channels = supabase_client.get_channels()
        log(f"Encontrados {len(channels)} canais para processar")
        
        if not channels:
            log("Nenhum canal encontrado", "ERROR")
            return False
        
        total_videos_processed = 0
        total_new = 0
        total_updated = 0
        total_skipped = 0
        total_errors = 0
        total_invalid_channel = 0
        
        for i, channel in enumerate(channels, 1):
            log(f"Processando canal {i}/{len(channels)}: {channel.name} (ID: {channel.channel_id})")
            
            try:
                # Obtém playlist de uploads
                playlist_id = youtube_extractor.get_upload_playlist_id(channel.channel_id)
                if not playlist_id:
                    log(f"  Erro: Não foi possível obter playlist do canal", "ERROR")
                    total_errors += 1
                    continue
                
                # Busca TODOS os vídeos do canal
                log(f"  Buscando todos os vídeos do canal...")
                videos_data = youtube_extractor.get_all_videos_from_playlist(playlist_id, start_date=None)
                
                if not videos_data:
                    log(f"  Nenhum vídeo encontrado no canal")
                    continue
                
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
                        total_invalid_channel += 1
                        continue
                    
                    # Busca vídeo existente
                    existing_video = supabase_client.get_video_by_id(video.video_id)
                    
                    if existing_video:
                        # Valida canal_id do vídeo existente
                        if existing_video.channel_id != channel.channel_id:
                            log(f"  [AVISO] Vídeo {video.video_id} existe mas pertence a outro canal (DB: {existing_video.channel_id}, Esperado: {channel.channel_id})", "WARNING")
                            total_invalid_channel += 1
                            continue
                        
                        # Verifica se há diferenças
                        if videos_differ(existing_video, video):
                            # Atualiza vídeo
                            if supabase_client.update_video(video):
                                total_updated += 1
                                total_videos_processed += 1
                                log(f"  [ATUALIZADO] {video.video_id}: {video.title[:50]}...")
                            else:
                                total_errors += 1
                                log(f"  [ERRO] Falha ao atualizar vídeo {video.video_id}", "ERROR")
                        else:
                            total_skipped += 1
                    else:
                        # Insere vídeo novo
                        if supabase_client.insert_video(video):
                            total_new += 1
                            total_videos_processed += 1
                            log(f"  [NOVO] {video.video_id}: {video.title[:50]}...")
                        else:
                            total_errors += 1
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
                
                log(f"  Canal processado: {total_new} novos, {total_updated} atualizados, {total_skipped} sem mudanças", "SUCCESS")
                
            except Exception as e:
                log(f"  Erro ao processar canal: {e}", "ERROR")
                import traceback
                traceback.print_exc()
                total_errors += 1
                continue
        
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
    success = run_full_update()
    sys.exit(0 if success else 1)

