"""
Versão CLI (linha de comando) do extrator de vídeos do YouTube
"""
import sys
import time
from datetime import datetime
import config
from api_key_manager import APIKeyManager
from supabase_client import SupabaseClient
from youtube_extractor import YouTubeExtractor
from scheduler import TaskScheduler
from utils import is_afternoon_time, is_night_time, parse_datetime, format_datetime


class ExtractorCLI:
    """Versão CLI do extrator"""
    
    def __init__(self):
        self.api_key_manager = APIKeyManager()
        self.supabase_client = SupabaseClient()
        self.youtube_extractor = None
        self.scheduler = TaskScheduler(self.run_extraction)
        self.is_running = False
        self.stop_requested = False
        
        # Carrega configuração
        self.scheduler.load_config()
    
    def log(self, message: str, level: str = "INFO"):
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
    
    def run_extraction(self):
        """Executa extração de vídeos"""
        if self.is_running:
            return
        
        self.is_running = True
        self.stop_requested = False
        
        try:
            # Determina modo baseado no horário atual
            current_hour = datetime.now().hour
            is_afternoon = is_afternoon_time(current_hour)
            is_night = is_night_time(current_hour)
            
            mode = "RETROATIVA" if is_afternoon else "ATUAL"
            self.log(f"Iniciando extração - Modo: {mode}")
            
            # Verifica se há chaves disponíveis
            if not self.api_key_manager.has_available_keys():
                self.log("Nenhuma chave de API disponível!", "ERROR")
                return
            
            # Reconstrói extrator com chave atual
            self.youtube_extractor = YouTubeExtractor(self.api_key_manager)
            
            # Busca canais
            if mode == "RETROATIVA":
                channels = self.supabase_client.get_channels_needing_old_videos()
                self.log(f"Encontrados {len(channels)} canais que precisam de vídeos antigos")
            else:
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
            
            for i, channel in enumerate(channels):
                if self.stop_requested:
                    self.log("Execução interrompida pelo usuário")
                    break
                
                self.log(f"Processando canal {i+1}/{len(channels)}: {channel.name} (ID: {channel.channel_id})")
                
                try:
                    # Obtém playlist de uploads
                    playlist_id = self.youtube_extractor.get_upload_playlist_id(channel.channel_id)
                    if not playlist_id:
                        self.log(f"  Erro: Não foi possível obter playlist do canal", "ERROR")
                        continue
                    
                    videos_found = []
                    
                    if mode == "RETROATIVA":
                        # Busca vídeos antigos retroativamente
                        # Se não tem oldest_video_date, usa newest_video_date
                        # Se não tem nenhum dos dois, busca todos os vídeos (start_date = None)
                        start_date = channel.oldest_video_date if channel.oldest_video_date else channel.newest_video_date
                        
                        if config.FETCH_ALL_VIDEOS_AT_ONCE:
                            # Busca TODOS os vídeos de uma vez
                            if start_date:
                                self.log(f"  Buscando TODOS os vídeos retroativamente a partir de {start_date}")
                            else:
                                self.log(f"  Buscando TODOS os vídeos do canal (primeira busca completa)")
                            
                            videos_data = self.youtube_extractor.get_all_videos_from_playlist(
                                playlist_id, start_date
                            )
                            
                            if not videos_data:
                                self.log(f"  Nenhum vídeo encontrado. Canal pode estar completo.")
                                continue
                            
                            self.log(f"  Encontrados {len(videos_data)} vídeos no total")
                        else:
                            # Busca gradual (50 por vez)
                            if start_date:
                                self.log(f"  Buscando vídeos retroativamente a partir de {start_date} (50 por execução)")
                            else:
                                self.log(f"  Buscando vídeos retroativamente (primeira busca - sem data inicial, 50 por execução)")
                            
                            videos_data = self.youtube_extractor.get_old_videos_retroactive(
                                playlist_id, start_date, max_videos=config.MAX_VIDEOS_PER_EXECUTION
                            )
                            
                            if not videos_data:
                                self.log(f"  Nenhum vídeo antigo encontrado. Verificando se canal está completo...")
                                continue
                            
                            self.log(f"  Encontrados {len(videos_data)} vídeos antigos (busca gradual)")
                    else:
                        # Busca vídeos novos
                        since_date = channel.newest_video_date
                        self.log(f"  Buscando vídeos novos desde {since_date}")
                        
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
                        if self.stop_requested:
                            break
                        
                        # Verifica se já existe
                        if self.supabase_client.video_exists(video.video_id):
                            total_existing += 1
                            continue
                        
                        # Insere vídeo
                        if self.supabase_client.insert_video(video):
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
                    
                    if oldest_date or newest_date:
                        current_oldest, current_newest = self.supabase_client.get_channel_video_dates(channel.channel_id)
                        
                        if oldest_date:
                            oldest_str = format_datetime(oldest_date)
                            if not current_oldest or oldest_date < parse_datetime(current_oldest):
                                update_oldest = oldest_str
                        
                        if newest_date:
                            newest_str = format_datetime(newest_date)
                            if not current_newest or newest_date > parse_datetime(current_newest):
                                update_newest = newest_str
                        
                        if update_oldest or update_newest:
                            self.supabase_client.update_channel_dates(
                                channel.channel_id, update_oldest, update_newest
                            )
                    
                    self.log(f"  Canal processado: {total_new} novos, {total_existing} já existentes", "SUCCESS")
                    
                    # Pausa entre canais
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
            
        except Exception as e:
            self.log(f"Erro na extração: {e}", "ERROR")
        finally:
            self.is_running = False
    
    def show_menu(self):
        """Mostra menu principal"""
        while True:
            print("\n" + "="*50)
            print("  EXTRATOR DE VÍDEOS DO YOUTUBE")
            print("="*50)
            print("1. Executar extração agora")
            print("2. Ver configuração de agendamento")
            print("3. Configurar agendamento")
            print("4. Gerenciar chaves de API")
            print("5. Sair")
            print("="*50)
            
            choice = input("\nEscolha uma opção: ").strip()
            
            if choice == "1":
                print("\nIniciando extração...")
                self.run_extraction()
                input("\nPressione Enter para continuar...")
            elif choice == "2":
                self.show_schedule_config()
            elif choice == "3":
                self.configure_schedule()
            elif choice == "4":
                self.manage_api_keys()
            elif choice == "5":
                print("Saindo...")
                break
            else:
                print("Opção inválida!")
    
    def show_schedule_config(self):
        """Mostra configuração de agendamento"""
        config_data = config.load_schedule_config()
        print("\n" + "-"*50)
        print("CONFIGURAÇÃO DE AGENDAMENTO")
        print("-"*50)
        print(f"Agendamento habilitado: {'Sim' if config_data.get('enabled') else 'Não'}")
        times = config_data.get('times', [])
        if times:
            print(f"Horários: {', '.join(times)}")
        else:
            print("Nenhum horário configurado")
        print("-"*50)
        input("\nPressione Enter para continuar...")
    
    def configure_schedule(self):
        """Configura agendamento"""
        print("\n" + "-"*50)
        print("CONFIGURAR AGENDAMENTO")
        print("-"*50)
        
        enabled = input("Habilitar agendamento? (s/n): ").strip().lower() == 's'
        
        times = []
        if enabled:
            time1 = input("Horário 1 (formato HH:MM, ex: 14:00): ").strip()
            if time1:
                times.append(time1)
            
            time2 = input("Horário 2 (formato HH:MM, deixe vazio se não quiser): ").strip()
            if time2:
                times.append(time2)
        
        self.scheduler.save_config(enabled, times)
        print("Configuração salva!", "SUCCESS")
        input("\nPressione Enter para continuar...")
    
    def manage_api_keys(self):
        """Gerencia chaves de API"""
        while True:
            print("\n" + "-"*50)
            print("GERENCIAR CHAVES DE API")
            print("-"*50)
            keys = self.api_key_manager.get_all_keys()
            for i, key in enumerate(keys, 1):
                display = f"...{key[-10:]}" if len(key) > 10 else key
                print(f"{i}. Chave {i}: {display}")
            print("-"*50)
            print("1. Adicionar chave")
            print("2. Remover chave")
            print("3. Voltar")
            
            choice = input("\nEscolha uma opção: ").strip()
            
            if choice == "1":
                key = input("Digite a chave de API: ").strip()
                if key:
                    self.api_key_manager.add_key(key)
                    self.log(f"Chave adicionada", "SUCCESS")
            elif choice == "2":
                if len(keys) <= 1:
                    print("É necessário ter pelo menos uma chave!")
                    continue
                try:
                    index = int(input(f"Digite o número da chave para remover (1-{len(keys)}): ")) - 1
                    if 0 <= index < len(keys):
                        self.api_key_manager.remove_key(keys[index])
                        self.log(f"Chave removida", "SUCCESS")
                    else:
                        print("Número inválido!")
                except ValueError:
                    print("Entrada inválida!")
            elif choice == "3":
                break
            else:
                print("Opção inválida!")


def main():
    """Função principal"""
    cli = ExtractorCLI()
    
    # Inicia agendador em thread separada
    if cli.scheduler.is_enabled():
        cli.scheduler.start()
        cli.log("Agendador iniciado")
    
    # Mostra menu
    cli.show_menu()
    
    # Para agendador
    cli.scheduler.stop()


if __name__ == "__main__":
    main()

