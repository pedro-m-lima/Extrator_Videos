"""
Interface desktop principal do extrator de vídeos do YouTube
"""
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, simpledialog
from datetime import datetime
import threading
import time
import config
from api_key_manager import APIKeyManager
from supabase_client import SupabaseClient
from youtube_extractor import YouTubeExtractor
from youtube_updater import YouTubeUpdater
from scheduler import TaskScheduler
from utils import parse_datetime, format_datetime


class ExtractorApp:
    """Aplicação principal com interface desktop"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Extrator de Vídeos do YouTube")
        self.root.geometry("900x700")
        
        # Componentes
        self.api_key_manager = APIKeyManager()
        self.supabase_client = SupabaseClient()
        self.youtube_extractor = None
        self.scheduler = TaskScheduler(self.run_extraction)
        
        # Estado
        self.is_running = False
        self.stop_requested = False
        
        # Interface
        self.setup_ui()
        
        # Carrega configuração
        self.scheduler.load_config()
        self.scheduler.start()
        
        # Atualiza status inicial
        self.update_status()
    
    def setup_ui(self):
        """Configura interface do usuário"""
        # Frame principal
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configuração de agendamento
        schedule_frame = ttk.LabelFrame(main_frame, text="Agendamento", padding="10")
        schedule_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        
        self.schedule_enabled = tk.BooleanVar(value=self.scheduler.is_enabled())
        ttk.Checkbutton(schedule_frame, text="Habilitar agendamento", 
                       variable=self.schedule_enabled).grid(row=0, column=0, sticky=tk.W)
        
        ttk.Label(schedule_frame, text="Horário 1:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.time1_var = tk.StringVar()
        times = self.scheduler.get_scheduled_times()
        if times:
            self.time1_var.set(times[0] if len(times) > 0 else "14:00")
        else:
            self.time1_var.set("14:00")
        ttk.Entry(schedule_frame, textvariable=self.time1_var, width=10).grid(row=1, column=1, sticky=tk.W, padx=5)
        
        ttk.Label(schedule_frame, text="Horário 2:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.time2_var = tk.StringVar()
        if times and len(times) > 1:
            self.time2_var.set(times[1])
        else:
            self.time2_var.set("22:00")
        ttk.Entry(schedule_frame, textvariable=self.time2_var, width=10).grid(row=2, column=1, sticky=tk.W, padx=5)
        
        ttk.Button(schedule_frame, text="Salvar Configuração", 
                  command=self.save_schedule).grid(row=3, column=0, columnspan=2, pady=10)
        
        # Gerenciamento de chaves API
        keys_frame = ttk.LabelFrame(main_frame, text="Chaves de API", padding="10")
        keys_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        
        self.keys_listbox = tk.Listbox(keys_frame, height=4)
        self.keys_listbox.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        self.update_keys_list()
        
        ttk.Button(keys_frame, text="Adicionar Chave", 
                  command=self.add_api_key).grid(row=1, column=0, padx=5)
        ttk.Button(keys_frame, text="Remover Chave", 
                  command=self.remove_api_key).grid(row=1, column=1, padx=5)
        
        # Controles
        control_frame = ttk.Frame(main_frame)
        control_frame.grid(row=2, column=0, columnspan=2, pady=10)
        
        self.run_button = ttk.Button(control_frame, text="Atualizar Agora", 
                                    command=self.run_extraction_manual)
        self.run_button.pack(side=tk.LEFT, padx=5)
        
        self.stop_button = ttk.Button(control_frame, text="Parar", 
                                     command=self.stop_extraction, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5)
        
        # Status
        self.status_var = tk.StringVar(value="Aguardando")
        status_label = ttk.Label(control_frame, textvariable=self.status_var)
        status_label.pack(side=tk.LEFT, padx=20)
        
        # Logs
        log_frame = ttk.LabelFrame(main_frame, text="Logs", padding="10")
        log_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=20, width=80)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # Configura grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(3, weight=1)
        keys_frame.columnconfigure(0, weight=1)
    
    def log(self, message: str, level: str = "INFO"):
        """Adiciona mensagem aos logs"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] [{level}] {message}\n"
        
        self.log_text.insert(tk.END, log_message)
        
        # Cores por nível
        if level == "ERROR":
            self.log_text.tag_add("error", f"end-{len(log_message)}c", "end-1c")
            self.log_text.tag_config("error", foreground="red")
        elif level == "SUCCESS":
            self.log_text.tag_add("success", f"end-{len(log_message)}c", "end-1c")
            self.log_text.tag_config("success", foreground="green")
        
        self.log_text.see(tk.END)
        self.root.update_idletasks()
    
    def update_keys_list(self):
        """Atualiza lista de chaves de API"""
        self.keys_listbox.delete(0, tk.END)
        keys = self.api_key_manager.get_all_keys()
        for i, key in enumerate(keys):
            # Mostra apenas últimos 10 caracteres para segurança
            display_key = f"Chave {i+1}: ...{key[-10:]}" if len(key) > 10 else f"Chave {i+1}: {key}"
            self.keys_listbox.insert(tk.END, display_key)
    
    def add_api_key(self):
        """Adiciona nova chave de API"""
        key = simpledialog.askstring("Adicionar Chave", "Digite a chave de API do YouTube:")
        if key:
            self.api_key_manager.add_key(key)
            self.update_keys_list()
            self.log(f"Chave de API adicionada", "SUCCESS")
    
    def remove_api_key(self):
        """Remove chave de API selecionada"""
        selection = self.keys_listbox.curselection()
        if not selection:
            messagebox.showwarning("Aviso", "Selecione uma chave para remover")
            return
        
        index = selection[0]
        keys = self.api_key_manager.get_all_keys()
        if len(keys) <= 1:
            messagebox.showwarning("Aviso", "É necessário ter pelo menos uma chave de API")
            return
        
        key = keys[index]
        self.api_key_manager.remove_key(key)
        self.update_keys_list()
        self.log(f"Chave de API removida", "SUCCESS")
    
    def save_schedule(self):
        """Salva configuração de agendamento"""
        enabled = self.schedule_enabled.get()
        time1 = self.time1_var.get()
        time2 = self.time2_var.get()
        
        times = [time1]
        if time2:
            times.append(time2)
        
        self.scheduler.save_config(enabled, times)
        self.log(f"Configuração de agendamento salva: {times}", "SUCCESS")
    
    def update_status(self):
        """Atualiza status na interface"""
        if self.is_running:
            self.status_var.set("Executando...")
            self.run_button.config(state=tk.DISABLED)
            self.stop_button.config(state=tk.NORMAL)
        else:
            self.status_var.set("Aguardando")
            self.run_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)
    
    def stop_extraction(self):
        """Para execução em andamento"""
        self.stop_requested = True
        self.log("Parando execução...", "INFO")
    
    def run_extraction_manual(self):
        """Inicia execução manual em thread separada"""
        if self.is_running:
            return
        
        thread = threading.Thread(target=self.run_extraction, daemon=True)
        thread.start()
    
    def run_extraction(self):
        """Executa extração de vídeos"""
        if self.is_running:
            return
        
        self.is_running = True
        self.stop_requested = False
        self.update_status()
        
        try:
            # Sempre usa modo ATUAL para extração manual e agendada
            mode = "ATUAL"
            self.log("=" * 60, "INFO")
            self.log(f"Iniciando extração - Modo: {mode} (FORÇADO - sempre busca vídeos novos)", "INFO")
            self.log("=" * 60, "INFO")
            
            # Verifica se há chaves disponíveis
            if not self.api_key_manager.has_available_keys():
                self.log("Nenhuma chave de API disponível!", "ERROR")
                return
            
            # Reconstrói extrator com chave atual
            self.youtube_extractor = YouTubeExtractor(self.api_key_manager)
            
            # Busca canais (sempre modo ATUAL - busca todos os canais)
            channels = self.supabase_client.get_channels()
            self.log(f"Encontrados {len(channels)} canais para processar", "INFO")
            
            if not channels:
                self.log("Nenhum canal encontrado", "INFO")
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
                    self.log("Execução interrompida pelo usuário", "INFO")
                    break
                
                self.log(f"Processando canal {i+1}/{len(channels)}: {channel.name} (ID: {channel.channel_id})", "INFO")
                
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
                        self.log(f"  [MODO ATUAL] Buscando vídeos novos desde {since_date}", "INFO")
                    else:
                        self.log(f"  [MODO ATUAL] Buscando vídeos novos (primeira busca - sem data inicial)", "INFO")
                    
                    videos_data = self.youtube_extractor.get_new_videos(playlist_id, since_date)
                    
                    if not videos_data:
                        self.log(f"  Nenhum vídeo novo encontrado", "INFO")
                        continue
                    
                    self.log(f"  Encontrados {len(videos_data)} vídeos novos", "INFO")
                    
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
                                    # Atualiza oldest_date (menor data)
                                    if not oldest_date or pub_date < oldest_date:
                                        oldest_date = pub_date
                                    # Atualiza newest_date (maior data)
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
                    self.log("=" * 60, "INFO")
                    self.log("Iniciando atualização de vídeos existentes...", "INFO")
                    youtube_updater = YouTubeUpdater(self.api_key_manager, self.supabase_client)
                    total_stats = youtube_updater.update_all_channels_videos(processed_channels, log_callback=self.log)
                    
                    self.log("=" * 60, "INFO")
                    self.log(f"Atualização de vídeos concluída!", "SUCCESS")
                    self.log(f"Total processados: {total_stats['total']}, Atualizados: {total_stats['updated']}, Sem mudanças: {total_stats['unchanged']}", "INFO")
                    
                    # Exibe quota do atualizador
                    updater_quota = youtube_updater.get_quota_info()
                    self.log(f"Quota adicional usada: {updater_quota['used']} unidades", "INFO")
                except Exception as e:
                    self.log(f"Erro ao atualizar vídeos: {e}", "ERROR")
            
        except Exception as e:
            self.log(f"Erro na extração: {e}", "ERROR")
        finally:
            self.is_running = False
            self.update_status()


def main():
    """Função principal"""
    root = tk.Tk()
    app = ExtractorApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()

