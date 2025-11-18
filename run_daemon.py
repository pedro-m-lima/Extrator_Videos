"""
Versão daemon do extrator - roda em background sem interface
"""
import sys
import time
import signal
from datetime import datetime
import config
from api_key_manager import APIKeyManager
from supabase_client import SupabaseClient
from youtube_extractor import YouTubeExtractor
from youtube_updater import YouTubeUpdater
from scheduler import TaskScheduler
from utils import parse_datetime, format_datetime


class ExtractorDaemon:
    """Versão daemon do extrator (roda em background)"""
    
    def __init__(self):
        self.api_key_manager = APIKeyManager()
        self.supabase_client = SupabaseClient()
        self.youtube_extractor = None
        self.scheduler = TaskScheduler(self.run_extraction)
        self.is_running = False
        self.stop_requested = False
        self.running = True
        
        # Carrega configuração
        self.scheduler.load_config()
        
        # Configura handlers de sinal para parar graciosamente
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handler para parar o daemon graciosamente"""
        print(f"\nRecebido sinal {signum}. Parando extrator...")
        self.running = False
        self.scheduler.stop()
        sys.exit(0)
    
    def log(self, message: str, level: str = "INFO"):
        """Adiciona mensagem aos logs"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prefix = {
            "INFO": "[INFO]",
            "SUCCESS": "[✓]",
            "ERROR": "[✗]",
            "WARNING": "[!]"
        }.get(level, "[INFO]")
        
        log_message = f"{timestamp} {prefix} {message}"
        print(log_message)
        sys.stdout.flush()
        
        # Também salva em arquivo de log
        try:
            with open('extrator.log', 'a', encoding='utf-8') as f:
                f.write(log_message + '\n')
        except:
            pass
    
    def run_extraction(self):
        """Executa extração de vídeos (mesmo código do main_cli)"""
        if self.is_running:
            return
        
        self.is_running = True
        self.stop_requested = False
        
        try:
            # Sempre usa modo ATUAL para extração manual e agendada
            mode = "ATUAL"
            self.log(f"Iniciando extração - Modo: {mode}")
            
            # Verifica se há chaves disponíveis
            if not self.api_key_manager.has_available_keys():
                self.log("Nenhuma chave de API disponível!", "ERROR")
                return
            
            # Reconstrói extrator com chave atual
            self.youtube_extractor = YouTubeExtractor(self.api_key_manager)
            
            # Busca canais (sempre modo ATUAL - busca todos os canais)
            channels = self.supabase_client.get_channels()
            self.log(f"Encontrados {len(channels)} canais para processar")
            
            if not channels:
                self.log("Nenhum canal encontrado")
                return
            
            # Ordena por prioridade
            channels.sort(key=lambda c: (
                -(c.priority + (2 if c.needs_old_videos else 0)),
                c.channel_id
            ), reverse=True)
            
            total_videos = 0
            total_new = 0
            total_existing = 0
            processed_channels = []  # Lista de canais processados para atualização
            
            for i, channel in enumerate(channels):
                if self.stop_requested or not self.running:
                    self.log("Execução interrompida")
                    break
                
                self.log(f"Processando canal {i+1}/{len(channels)}: {channel.name} (ID: {channel.channel_id})")
                
                try:
                    # Obtém playlist de uploads
                    playlist_id = self.youtube_extractor.get_upload_playlist_id(channel.channel_id)
                    if not playlist_id:
                        self.log(f"  Erro: Não foi possível obter playlist do canal", "ERROR")
                        continue
                    
                    # Sempre busca vídeos novos (MODO ATUAL)
                    since_date = channel.newest_video_date
                    if since_date:
                        self.log(f"  [MODO ATUAL] Buscando vídeos novos desde {since_date}")
                    else:
                        self.log(f"  [MODO ATUAL] Buscando vídeos novos (primeira busca - sem data inicial)")
                    
                    videos_data = self.youtube_extractor.get_new_videos(playlist_id, since_date)
                    
                    if not videos_data:
                        self.log(f"  Nenhum vídeo novo encontrado")
                        continue
                    
                    self.log(f"  Encontrados {len(videos_data)} vídeos novos")
                    
                    # Processa vídeos
                    videos = self.youtube_extractor.process_videos(videos_data, channel.channel_id)
                    
                    # Insere no banco
                    oldest_date = None
                    newest_date = None
                    
                    for video in videos:
                        if self.stop_requested or not self.running:
                            break
                        
                        if self.supabase_client.video_exists(video.video_id):
                            total_existing += 1
                            continue
                        
                        if self.supabase_client.insert_video(video):
                            total_new += 1
                            total_videos += 1
                            
                            if video.published_at:
                                pub_date = parse_datetime(video.published_at)
                                if pub_date:
                                    if not oldest_date or pub_date < oldest_date:
                                        oldest_date = pub_date
                                    if not newest_date or pub_date > newest_date:
                                        newest_date = pub_date
                    
                    # Atualiza datas do canal (sempre modo ATUAL - atualiza newest_date)
                    update_newest = None
                    
                    if newest_date:
                        # Busca atual: atualiza newest_date
                        update_newest = format_datetime(newest_date)
                        
                        # Compara com data atual do canal
                        current_oldest, current_newest = self.supabase_client.get_channel_video_dates(channel.channel_id)
                        
                        newest_str = format_datetime(newest_date)
                        if not current_newest or newest_date > parse_datetime(current_newest):
                            update_newest = newest_str
                            
                            if update_newest:
                                self.supabase_client.update_channel_dates(
                                    channel.channel_id, None, update_newest
                                )
                    
                    self.log(f"  Canal processado: {total_new} novos, {total_existing} já existentes", "SUCCESS")
                    
                    # Adiciona canal à lista de processados
                    processed_channels.append(channel.channel_id)
                    
                    time.sleep(config.CHANNEL_DELAY)
                    
                except Exception as e:
                    self.log(f"  Erro ao processar canal: {e}", "ERROR")
                    continue
            
            self.log(f"Extração concluída! Total: {total_videos} vídeos ({total_new} novos, {total_existing} já existentes)", "SUCCESS")
            
            # Exibe informações de quota
            if self.youtube_extractor:
                quota_info = self.youtube_extractor.get_quota_info()
                self.log(f"Quota da API: {quota_info['used']}/{quota_info['limit']} usada ({quota_info['percentage_used']:.1f}%)", "INFO")
                self.log(f"Quota restante: {quota_info['remaining']} unidades", "INFO")
                breakdown = quota_info['breakdown']
                if breakdown['channels_list'] > 0 or breakdown['playlist_items'] > 0 or breakdown['videos_list'] > 0:
                    self.log(f"Detalhamento: channels.list={breakdown['channels_list']}, playlistItems.list={breakdown['playlist_items']}, videos.list={breakdown['videos_list']}", "INFO")
            
            # Atualiza vídeos dos canais processados
            if processed_channels and not self.stop_requested and self.running:
                try:
                    self.log("=" * 60)
                    self.log("Iniciando atualização de vídeos existentes...")
                    youtube_updater = YouTubeUpdater(self.api_key_manager, self.supabase_client)
                    total_stats = youtube_updater.update_all_channels_videos(processed_channels, log_callback=self.log)
                    
                    self.log("=" * 60)
                    self.log(f"Atualização de vídeos concluída!", "SUCCESS")
                    self.log(f"Total processados: {total_stats['total']}, Atualizados: {total_stats['updated']}, Sem mudanças: {total_stats['unchanged']}")
                    
                    # Exibe quota do atualizador
                    updater_quota = youtube_updater.get_quota_info()
                    self.log(f"Quota adicional usada: {updater_quota['used']} unidades")
                except Exception as e:
                    self.log(f"Erro ao atualizar vídeos: {e}", "ERROR")
            
        except Exception as e:
            self.log(f"Erro na extração: {e}", "ERROR")
        finally:
            self.is_running = False
    
    def run(self):
        """Loop principal do daemon"""
        self.log("Extrator iniciado em modo daemon")
        
        # Inicia agendador
        if self.scheduler.is_enabled():
            self.scheduler.start()
            times = self.scheduler.get_scheduled_times()
            self.log(f"Agendamento habilitado. Horários: {', '.join(times)}")
        else:
            self.log("Agendamento desabilitado")
        
        # Loop principal - fica rodando
        try:
            while self.running:
                time.sleep(60)  # Verifica a cada minuto
        except KeyboardInterrupt:
            self.log("Interrompido pelo usuário")
        finally:
            self.scheduler.stop()
            self.log("Extrator parado")


def main():
    """Função principal do daemon"""
    daemon = ExtractorDaemon()
    daemon.run()


if __name__ == "__main__":
    main()

