"""
Gerenciador de agendamento de tarefas
"""
import schedule
import threading
import time
from datetime import datetime
import config
from typing import Callable, Optional


class TaskScheduler:
    """Gerencia agendamento de execuções automáticas"""
    
    def __init__(self, task_callback: Callable):
        self.task_callback = task_callback
        self.running = False
        self.thread = None
        self.config = config.load_schedule_config()
    
    def load_config(self):
        """Carrega configuração de agendamento"""
        self.config = config.load_schedule_config()
        self._update_schedule()
    
    def save_config(self, enabled: bool, times: list):
        """Salva configuração de agendamento"""
        self.config = {
            'enabled': enabled,
            'times': times
        }
        config.save_schedule_config(self.config)
        self._update_schedule()
    
    def _update_schedule(self):
        """Atualiza agendamentos baseado na configuração"""
        schedule.clear()
        
        if not self.config.get('enabled', False):
            return
        
        times = self.config.get('times', [])
        for time_str in times:
            try:
                # Formato esperado: "HH:MM"
                schedule.every().day.at(time_str).do(self._execute_task)
            except Exception as e:
                print(f"Erro ao agendar horário {time_str}: {e}")
    
    def _execute_task(self):
        """Executa tarefa agendada"""
        if self.task_callback:
            try:
                self.task_callback()
            except Exception as e:
                print(f"Erro ao executar tarefa agendada: {e}")
    
    def start(self):
        """Inicia thread de agendamento"""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
    
    def stop(self):
        """Para thread de agendamento"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)
    
    def _run(self):
        """Loop principal do agendador"""
        while self.running:
            schedule.run_pending()
            time.sleep(1)
    
    def get_next_run_time(self) -> Optional[datetime]:
        """Retorna próxima execução agendada"""
        jobs = schedule.jobs
        if not jobs:
            return None
        
        next_run = min(job.next_run for job in jobs)
        return next_run
    
    def is_enabled(self) -> bool:
        """Verifica se agendamento está habilitado"""
        return self.config.get('enabled', False)
    
    def get_scheduled_times(self) -> list:
        """Retorna lista de horários agendados"""
        return self.config.get('times', [])

