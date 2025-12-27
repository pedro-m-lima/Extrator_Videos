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
from youtube_updater import YouTubeUpdater
from scheduler import TaskScheduler
from utils import parse_datetime, format_datetime


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
                    
                    # Atualiza datas do canal (sempre modo ATUAL - atualiza newest_date)
                    update_newest = None
                    
                    if newest_date:
                        # Busca atual: atualiza newest_date
                        update_newest = format_datetime(newest_date)
                        
                        # Compara com data atual do canal
                        current_oldest, current_newest = self.supabase_client.get_channel_video_dates(channel.channel_id)
                        
                        newest_str = format_datetime(newest_date)
                        # Verifica se current_newest é válido e pode ser parseado antes de comparar
                        current_newest_dt = parse_datetime(current_newest) if current_newest else None
                        if not current_newest_dt or newest_date > current_newest_dt:
                            update_newest = newest_str
                            
                            if update_newest:
                                self.supabase_client.update_channel_dates(
                                    channel.channel_id, None, update_newest
                                )
                    
                    self.log(f"  Canal processado: {total_new} novos, {total_existing} já existentes", "SUCCESS")
                    
                    # Adiciona canal à lista de processados
                    processed_channels.append(channel.channel_id)
                    
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
            
            # Atualiza vídeos dos canais processados
            if processed_channels and not self.stop_requested:
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
    
    def update_channel_videos(self):
        """Atualiza todos os vídeos de um canal específico"""
        try:
            # Busca todos os canais
            channels = self.supabase_client.get_channels()
            
            if not channels:
                self.log("Nenhum canal encontrado no banco de dados", "ERROR")
                input("\nPressione Enter para continuar...")
                return
            
            # Lista canais para escolha
            print("\n" + "="*50)
            print("  CANAIS DISPONÍVEIS")
            print("="*50)
            for i, channel in enumerate(channels, 1):
                print(f"{i}. {channel.name} (ID: {channel.channel_id})")
            print("="*50)
            
            # Solicita escolha do canal
            try:
                choice = int(input(f"\nEscolha o canal (1-{len(channels)}): ").strip())
                if choice < 1 or choice > len(channels):
                    print("Opção inválida!")
                    input("\nPressione Enter para continuar...")
                    return
                
                selected_channel = channels[choice - 1]
                
                # Confirmação
                print(f"\nVocê escolheu: {selected_channel.name}")
                confirm = input("Deseja atualizar TODOS os vídeos deste canal? (s/n): ").strip().lower()
                if confirm != 's':
                    print("Operação cancelada.")
                    input("\nPressione Enter para continuar...")
                    return
                
                # Verifica se há chaves disponíveis
                if not self.api_key_manager.has_available_keys():
                    self.log("Nenhuma chave de API disponível!", "ERROR")
                    input("\nPressione Enter para continuar...")
                    return
                
                # Reconstrói extrator com chave atual
                self.youtube_extractor = YouTubeExtractor(self.api_key_manager)
                
                self.log(f"Iniciando atualização completa do canal: {selected_channel.name}")
                
                # Obtém playlist de uploads
                playlist_id = self.youtube_extractor.get_upload_playlist_id(selected_channel.channel_id)
                if not playlist_id:
                    self.log(f"Erro: Não foi possível obter playlist do canal", "ERROR")
                    input("\nPressione Enter para continuar...")
                    return
                
                # Busca TODOS os vídeos (sem filtro de data)
                self.log("Buscando TODOS os vídeos do canal (isso pode levar alguns minutos)...")
                videos_data = self.youtube_extractor.get_all_videos_from_playlist(playlist_id, start_date=None)
                
                if not videos_data:
                    self.log("Nenhum vídeo encontrado no canal", "WARNING")
                    input("\nPressione Enter para continuar...")
                    return
                
                self.log(f"Encontrados {len(videos_data)} vídeos no total")
                
                # Processa vídeos
                self.log("Processando vídeos...")
                videos = self.youtube_extractor.process_videos(videos_data, selected_channel.channel_id)
                
                # Insere/atualiza no banco
                total_videos = 0
                total_new = 0
                total_existing = 0
                oldest_date = None
                newest_date = None
                
                for i, video in enumerate(videos):
                    if i % 10 == 0:
                        self.log(f"Processando vídeo {i+1}/{len(videos)}...")
                    
                    # Verifica se já existe
                    exists = self.supabase_client.video_exists(video.video_id)
                    
                    if exists:
                        total_existing += 1
                    else:
                        # Insere vídeo novo
                        if self.supabase_client.insert_video(video):
                            total_new += 1
                            total_videos += 1
                    
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
                    self.supabase_client.update_channel_dates(
                        selected_channel.channel_id,
                        oldest_date=format_datetime(oldest_date),
                        newest_date=format_datetime(newest_date)
                    )
                    self.log(f"Datas do canal atualizadas: {format_datetime(oldest_date)} até {format_datetime(newest_date)}")
                
                self.log(f"Atualização concluída!", "SUCCESS")
                self.log(f"Total: {len(videos)} vídeos processados")
                self.log(f"Novos: {total_new}, Já existentes: {total_existing}")
                
                # Exibe informações de quota
                if self.youtube_extractor:
                    quota_info = self.youtube_extractor.get_quota_info()
                    self.log(f"Quota da API: {quota_info['used']}/{quota_info['limit']} usada ({quota_info['percentage_used']:.1f}%)", "INFO")
                    self.log(f"Quota restante: {quota_info['remaining']} unidades", "INFO")
                    breakdown = quota_info['breakdown']
                    if breakdown['channels_list'] > 0 or breakdown['playlist_items'] > 0 or breakdown['videos_list'] > 0:
                        self.log(f"Detalhamento: channels.list={breakdown['channels_list']}, playlistItems.list={breakdown['playlist_items']}, videos.list={breakdown['videos_list']}", "INFO")
                
            except ValueError:
                print("Entrada inválida! Digite um número.")
                input("\nPressione Enter para continuar...")
            except Exception as e:
                self.log(f"Erro ao atualizar canal: {e}", "ERROR")
                import traceback
                traceback.print_exc()
                input("\nPressione Enter para continuar...")
                
        except Exception as e:
            self.log(f"Erro: {e}", "ERROR")
            input("\nPressione Enter para continuar...")
    
    def show_menu(self):
        """Mostra menu principal"""
        while True:
            print("\n" + "="*50)
            print("  EXTRATOR DE VÍDEOS DO YOUTUBE")
            print("="*50)
            print("1. Executar extração agora")
            print("2. Atualizar todos os vídeos de um canal específico")
            print("3. Ver configuração de agendamento")
            print("4. Configurar agendamento")
            print("5. Gerenciar chaves de API")
            print("6. Sair")
            print("="*50)
            
            choice = input("\nEscolha uma opção: ").strip()
            
            if choice == "1":
                print("\nIniciando extração...")
                self.run_extraction()
                input("\nPressione Enter para continuar...")
            elif choice == "2":
                self.update_channel_videos()
            elif choice == "3":
                self.show_schedule_config()
            elif choice == "4":
                self.configure_schedule()
            elif choice == "5":
                self.manage_api_keys()
            elif choice == "6":
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

