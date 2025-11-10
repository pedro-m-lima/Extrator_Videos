"""
Script standalone para rodar no GitHub Actions
Executa uma única extração sem interface
"""
import sys
import os
from datetime import datetime
import config
from api_key_manager import APIKeyManager
from supabase_client import SupabaseClient
from youtube_extractor import YouTubeExtractor
from utils import is_afternoon_time, is_night_time, parse_datetime, format_datetime


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


def run_extraction():
    """Executa extração de vídeos"""
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
        # Substitui chaves carregadas pelas do ambiente
        if api_keys:
            api_key_manager.keys = api_keys
            api_key_manager.current_key_index = 0
        
        supabase_client = SupabaseClient()
        youtube_extractor = YouTubeExtractor(api_key_manager)
        
        # Determina modo baseado no horário atual (UTC no GitHub Actions)
        current_hour = datetime.utcnow().hour
        is_afternoon = is_afternoon_time(current_hour)
        is_night = is_night_time(current_hour)
        
        mode = "RETROATIVA" if is_afternoon else "ATUAL"
        log(f"Iniciando extração - Modo: {mode}")
        
        # Verifica se há chaves disponíveis
        if not api_key_manager.has_available_keys():
            log("Nenhuma chave de API disponível!", "ERROR")
            return False
        
        # Busca canais
        if mode == "RETROATIVA":
            channels = supabase_client.get_channels_needing_old_videos()
            log(f"Encontrados {len(channels)} canais que precisam de vídeos antigos")
        else:
            channels = supabase_client.get_channels()
            log(f"Encontrados {len(channels)} canais para processar")
        
        if not channels:
            log("Nenhum canal encontrado")
            return True
        
        # Ordena por prioridade
        channels.sort(key=lambda c: (
            -(getattr(c, 'priority', 0) + (2 if getattr(c, 'needs_old_videos', False) else 0)),
            c.channel_id
        ), reverse=True)
        
        total_videos = 0
        total_new = 0
        total_existing = 0
        
        for i, channel in enumerate(channels):
            log(f"Processando canal {i+1}/{len(channels)}: {channel.name} (ID: {channel.channel_id})")
            
            try:
                # Obtém playlist de uploads
                playlist_id = youtube_extractor.get_upload_playlist_id(channel.channel_id)
                if not playlist_id:
                    log(f"  Erro: Não foi possível obter playlist do canal", "ERROR")
                    continue
                
                videos_found = []
                
                if mode == "RETROATIVA":
                    # Busca vídeos antigos retroativamente
                    start_date = channel.oldest_video_date if channel.oldest_video_date else channel.newest_video_date
                    
                    if config.FETCH_ALL_VIDEOS_AT_ONCE:
                        # Busca TODOS os vídeos de uma vez
                        if start_date:
                            log(f"  Buscando TODOS os vídeos retroativamente a partir de {start_date}")
                        else:
                            log(f"  Buscando TODOS os vídeos do canal (primeira busca completa)")
                        
                        videos_data = youtube_extractor.get_all_videos_from_playlist(
                            playlist_id, start_date
                        )
                        
                        if not videos_data:
                            log(f"  Nenhum vídeo encontrado. Canal pode estar completo.")
                            continue
                        
                        log(f"  Encontrados {len(videos_data)} vídeos no total")
                    else:
                        # Busca gradual (50 por vez)
                        if start_date:
                            log(f"  Buscando vídeos retroativamente a partir de {start_date} (50 por execução)")
                        else:
                            log(f"  Buscando vídeos retroativamente (primeira busca - sem data inicial, 50 por execução)")
                        
                        videos_data = youtube_extractor.get_old_videos_retroactive(
                            playlist_id, start_date, max_videos=config.MAX_VIDEOS_PER_EXECUTION
                        )
                        
                        if not videos_data:
                            log(f"  Nenhum vídeo antigo encontrado. Verificando se canal está completo...")
                            continue
                        
                        log(f"  Encontrados {len(videos_data)} vídeos antigos (busca gradual)")
                else:
                    # Busca vídeos novos
                    since_date = channel.newest_video_date
                    log(f"  Buscando vídeos novos desde {since_date}")
                    
                    videos_data = youtube_extractor.get_new_videos(playlist_id, since_date)
                    
                    if not videos_data:
                        log(f"  Nenhum vídeo novo encontrado")
                        continue
                    
                    log(f"  Encontrados {len(videos_data)} vídeos novos")
                
                # Processa vídeos
                videos = youtube_extractor.process_videos(videos_data, channel.channel_id)
                
                # Insere no banco
                oldest_date = None
                newest_date = None
                
                for video in videos:
                    # Verifica se já existe
                    if supabase_client.video_exists(video.video_id):
                        total_existing += 1
                        continue
                    
                    # Insere vídeo
                    if supabase_client.insert_video(video):
                        total_new += 1
                        total_videos += 1
                        
                        # Atualiza datas
                        if video.published_at:
                            pub_date = parse_datetime(video.published_at)
                            if pub_date:
                                if not oldest_date or pub_date < oldest_date:
                                    oldest_date = pub_date
                                if not newest_date or pub_date > newest_date:
                                    newest_date = pub_date
                
                # Atualiza datas do canal
                update_oldest = None
                update_newest = None
                
                if mode == "RETROATIVA" and oldest_date:
                    update_oldest = format_datetime(oldest_date)
                elif mode == "ATUAL" and newest_date:
                    update_newest = format_datetime(newest_date)
                
                if update_oldest or update_newest:
                    supabase_client.update_channel_dates(
                        channel.channel_id,
                        oldest_date=update_oldest,
                        newest_date=update_newest
                    )
                
                log(f"  Canal processado: {total_new} novos, {total_existing} já existentes", "SUCCESS")
                
            except Exception as e:
                log(f"  Erro ao processar canal: {e}", "ERROR")
                continue
        
        log(f"Extração concluída! Total: {total_videos} vídeos ({total_new} novos, {total_existing} já existentes)", "SUCCESS")
        
        # Exibe informações de quota
        quota_info = youtube_extractor.get_quota_info()
        log(f"Quota da API: {quota_info['used']}/{quota_info['limit']} usada ({quota_info['percentage_used']:.1f}%)", "INFO")
        log(f"Quota restante: {quota_info['remaining']} unidades", "INFO")
        breakdown = quota_info['breakdown']
        if breakdown['channels_list'] > 0 or breakdown['playlist_items'] > 0 or breakdown['videos_list'] > 0:
            log(f"Detalhamento: channels.list={breakdown['channels_list']}, playlistItems.list={breakdown['playlist_items']}, videos.list={breakdown['videos_list']}", "INFO")
        
        return True
        
    except Exception as e:
        log(f"Erro na extração: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_extraction()
    sys.exit(0 if success else 1)

