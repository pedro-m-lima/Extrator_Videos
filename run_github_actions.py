"""
Script standalone para rodar no GitHub Actions
Executa uma única extração sem interface
Suporta processamento paralelo com 3 workers simultâneos
"""
import sys
import os
import gc
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def process_single_channel(
    channel: Channel,
    mode: str,
    channel_index: int,
    total_channels: int,
    api_keys: list = None
) -> dict:
    """Processa um único canal e retorna estatísticas"""
    channel_stats = {
        'channel_id': channel.channel_id,
        'channel_name': channel.name,
        'new': 0,
        'existing': 0,
        'errors': 0
    }
    
    # Cria instâncias locais dos clientes para evitar problemas de thread-safety
    try:
        api_key_manager = APIKeyManager()
        # Configura chaves das variáveis de ambiente se fornecidas
        if api_keys:
            api_key_manager.keys = api_keys
            api_key_manager.current_key_index = 0
            # Reinicializa quota_tracking com as novas chaves
            api_key_manager.quota_tracking = {key: {'used': 0, 'exceeded': False} for key in api_keys}
        
        # Verifica se há chaves disponíveis
        if not api_key_manager.has_available_keys():
            log(f"  Nenhuma chave de API disponível para {channel.name}", "ERROR")
            channel_stats['errors'] = 1
            return channel_stats
        
        youtube_extractor = YouTubeExtractor(api_key_manager)
        supabase_client = SupabaseClient()
    except Exception as e:
        log(f"  Erro ao inicializar clientes para {channel.name}: {e}", "ERROR")
        channel_stats['errors'] = 1
        return channel_stats
    
    try:
        log(f"[{channel_index}/{total_channels}] Processando: {channel.name} (ID: {channel.channel_id})")
        
        # Obtém playlist de uploads
        playlist_id = youtube_extractor.get_upload_playlist_id(channel.channel_id)
        if not playlist_id:
            log(f"  Erro: Não foi possível obter playlist do canal", "ERROR")
            channel_stats['errors'] = 1
            return channel_stats
        
        # Sempre busca vídeos novos (MODO ATUAL)
        since_date = channel.newest_video_date
        if since_date:
            log(f"  [MODO ATUAL] Buscando vídeos novos desde {since_date}")
        else:
            log(f"  [MODO ATUAL] Buscando vídeos novos (primeira busca - sem data inicial)")
        
        videos_data = youtube_extractor.get_new_videos(playlist_id, since_date)
        
        if not videos_data:
            log(f"  Nenhum vídeo novo encontrado")
            return channel_stats
        
        log(f"  Encontrados {len(videos_data)} vídeos novos")
        
        # Processa vídeos
        videos = youtube_extractor.process_videos(videos_data, channel.channel_id)
        
        # Insere no banco
        oldest_date = None
        newest_date = None
        
        for video in videos:
            # Verifica se já existe
            if supabase_client.video_exists(video.video_id):
                channel_stats['existing'] += 1
                continue
            
            # Insere vídeo
            if supabase_client.insert_video(video):
                channel_stats['new'] += 1
                
                # Atualiza datas
                if video.published_at:
                    pub_date = parse_datetime(video.published_at)
                    if pub_date:
                        if not oldest_date or pub_date < oldest_date:
                            oldest_date = pub_date
                        if not newest_date or pub_date > newest_date:
                            newest_date = pub_date
            else:
                channel_stats['errors'] += 1
        
        # Atualiza datas do canal (sempre modo ATUAL - atualiza newest_date)
        update_newest = None
        
        if newest_date:
            # Busca atual: atualiza newest_date
            update_newest = format_datetime(newest_date)
            
            # Compara com data atual do canal
            current_oldest, current_newest = supabase_client.get_channel_video_dates(channel.channel_id)
            
            newest_str = format_datetime(newest_date)
            if not current_newest or newest_date > parse_datetime(current_newest):
                update_newest = newest_str
                
                if update_newest:
                    supabase_client.update_channel_dates(
                        channel.channel_id,
                        oldest_date=None,
                        newest_date=update_newest
                    )
        
        log(f"  [{channel_index}/{total_channels}] ✓ {channel.name}: {channel_stats['new']} novos, {channel_stats['existing']} já existentes", "SUCCESS")
        
    except Exception as e:
        log(f"  [{channel_index}/{total_channels}] ✗ Erro ao processar canal {channel.name}: {e}", "ERROR")
        channel_stats['errors'] += 1
    finally:
        # Limpa referências dos clientes
        try:
            del youtube_extractor
            del supabase_client
            del api_key_manager
        except:
            pass
    
    return channel_stats


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
        
        # Sempre usa modo ATUAL para extração (busca vídeos novos)
        mode = "ATUAL"
        log("=" * 60)
        log(f"Iniciando extração - Modo: {mode} (FORÇADO - sempre busca vídeos novos)")
        log("=" * 60)
        
        # Inicializa componentes apenas para buscar canais e verificar quota
        api_key_manager = APIKeyManager()
        # Substitui chaves carregadas pelas do ambiente
        if api_keys:
            api_key_manager.keys = api_keys
            api_key_manager.current_key_index = 0
        
        # Verifica se há chaves disponíveis
        if not api_key_manager.has_available_keys():
            log("Nenhuma chave de API disponível!", "ERROR")
            return False
        
        # Inicializa cliente do Supabase apenas para buscar canais
        supabase_client = SupabaseClient()
        
        # Busca canais (sempre modo ATUAL - busca todos os canais)
        channels = supabase_client.get_channels()
        log(f"Encontrados {len(channels)} canais para processar")
        
        # Limpa referências dos clientes principais (cada thread criará os seus)
        del supabase_client
        del api_key_manager
        gc.collect()
        
        if not channels:
            log("Nenhum canal encontrado")
            return True
        
        # Ordena por prioridade
        channels.sort(key=lambda c: (
            -(getattr(c, 'priority', 0) + (2 if getattr(c, 'needs_old_videos', False) else 0)),
            c.channel_id
        ), reverse=True)
        
        log(f"Processamento paralelo: 3 workers simultâneos")
        
        total_videos = 0
        total_new = 0
        total_existing = 0
        total_errors = 0
        
        # Processa canais em paralelo com 3 workers
        # Cada thread cria seus próprios clientes para evitar problemas de thread-safety
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Submete todas as tarefas, passando as chaves de API para cada thread
            future_to_channel = {
                executor.submit(
                    process_single_channel,
                    channel,
                    mode,
                    i + 1,
                    len(channels),
                    api_keys  # Passa as chaves carregadas das variáveis de ambiente
                ): channel
                for i, channel in enumerate(channels)
            }
            
            # Processa resultados conforme completam
            for future in as_completed(future_to_channel):
                channel = future_to_channel[future]
                try:
                    stats = future.result()
                    total_new += stats['new']
                    total_existing += stats['existing']
                    total_errors += stats['errors']
                    total_videos += stats['new']
                except Exception as e:
                    log(f"Erro ao processar resultado do canal {channel.name}: {e}", "ERROR")
                    total_errors += 1
            
            # Limpa referências
            del future_to_channel
            gc.collect()
        
        log(f"Extração concluída! Total: {total_videos} vídeos ({total_new} novos, {total_existing} já existentes, {total_errors} erros)", "SUCCESS")
        
        # Exibe informações de quota (cria instância temporária)
        try:
            api_key_manager = APIKeyManager()
            if api_keys:
                api_key_manager.keys = api_keys
                api_key_manager.current_key_index = 0
            youtube_extractor = YouTubeExtractor(api_key_manager)
            quota_info = youtube_extractor.get_quota_info()
            log(f"Quota da API: {quota_info['used']}/{quota_info['limit']} usada ({quota_info['percentage_used']:.1f}%)", "INFO")
            log(f"Quota restante: {quota_info['remaining']} unidades", "INFO")
            breakdown = quota_info['breakdown']
            if breakdown['channels_list'] > 0 or breakdown['playlist_items'] > 0 or breakdown['videos_list'] > 0:
                log(f"Detalhamento: channels.list={breakdown['channels_list']}, playlistItems.list={breakdown['playlist_items']}, videos.list={breakdown['videos_list']}", "INFO")
            del youtube_extractor
            del api_key_manager
        except Exception as e:
            log(f"Não foi possível obter informações de quota: {e}", "WARNING")
        
        # Atualiza historical_metrics (não quebra se houver erro)
        try:
            log("", "INFO")
            log("Atualizando historical_metrics...", "INFO")
            from historical_metrics_aggregator import HistoricalMetricsAggregator
            supabase_client = SupabaseClient()
            aggregator = HistoricalMetricsAggregator(supabase_client)
            
            # Processa mês atual
            stats = aggregator.process_current_month()
            log(f"Historical metrics: {stats['channels_processed']} processados, {stats['channels_updated']} atualizados, {stats['channels_created']} criados", "SUCCESS")
            
            # Se for último dia do mês, cria entradas do próximo mês
            from datetime import date
            from calendar import monthrange
            today = date.today()
            last_day = monthrange(today.year, today.month)[1]
            
            if today.day == last_day:
                log(f"Último dia do mês detectado, criando entradas para o próximo mês...", "INFO")
                next_month_stats = aggregator.create_next_month_entries()
                log(f"Entradas criadas: {next_month_stats['entries_created']}", "SUCCESS")
            
            del aggregator
            del supabase_client
        except Exception as e:
            log(f"Erro ao atualizar historical_metrics (não crítico): {e}", "WARNING")
            import traceback
            traceback.print_exc()
        
        return True
        
    except Exception as e:
        log(f"Erro na extração: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_extraction()
    sys.exit(0 if success else 1)

