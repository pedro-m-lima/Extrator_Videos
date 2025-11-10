"""
Gerenciador de múltiplas chaves de API do YouTube
"""
import config
from typing import List, Optional


class APIKeyManager:
    """Gerencia múltiplas chaves de API com rotação automática"""
    
    def __init__(self):
        self.keys = config.load_api_keys()
        self.current_key_index = 0
        self.quota_tracking = {key: {'used': 0, 'exceeded': False} for key in self.keys}
    
    def get_current_key(self) -> Optional[str]:
        """Retorna a chave atual"""
        if not self.keys:
            return None
        return self.keys[self.current_key_index]
    
    def get_next_available_key(self) -> Optional[str]:
        """Retorna próxima chave disponível (não excedida)"""
        available_keys = [
            key for key, tracking in self.quota_tracking.items()
            if not tracking['exceeded']
        ]
        
        if not available_keys:
            return None
        
        # Encontra índice da primeira chave disponível
        for i, key in enumerate(self.keys):
            if key in available_keys:
                self.current_key_index = i
                return key
        
        return None
    
    def mark_quota_exceeded(self, key: Optional[str] = None):
        """Marca chave como excedida (quota esgotada)"""
        if key is None:
            key = self.get_current_key()
        
        if key and key in self.quota_tracking:
            self.quota_tracking[key]['exceeded'] = True
    
    def add_quota_usage(self, key: Optional[str] = None, amount: int = 1):
        """Adiciona uso de quota para uma chave"""
        if key is None:
            key = self.get_current_key()
        
        if key and key in self.quota_tracking:
            self.quota_tracking[key]['used'] += amount
    
    def rotate_key(self) -> bool:
        """Rotaciona para próxima chave disponível"""
        next_key = self.get_next_available_key()
        if next_key:
            self.current_key_index = self.keys.index(next_key)
            return True
        return False
    
    def handle_quota_error(self) -> bool:
        """Trata erro de quota excedida, rotaciona chave se possível"""
        current_key = self.get_current_key()
        if current_key:
            self.mark_quota_exceeded(current_key)
            return self.rotate_key()
        return False
    
    def add_key(self, key: str):
        """Adiciona nova chave de API"""
        if key not in self.keys:
            self.keys.append(key)
            self.quota_tracking[key] = {'used': 0, 'exceeded': False}
            config.save_api_keys(self.keys)
    
    def remove_key(self, key: str):
        """Remove chave de API"""
        if key in self.keys and len(self.keys) > 1:
            self.keys.remove(key)
            if key in self.quota_tracking:
                del self.quota_tracking[key]
            config.save_api_keys(self.keys)
            # Ajusta índice se necessário
            if self.current_key_index >= len(self.keys):
                self.current_key_index = 0
    
    def get_all_keys(self) -> List[str]:
        """Retorna lista de todas as chaves"""
        return self.keys.copy()
    
    def get_quota_info(self) -> dict:
        """Retorna informações de quota de todas as chaves"""
        return {
            key: {
                'used': tracking['used'],
                'exceeded': tracking['exceeded'],
                'available': not tracking['exceeded']
            }
            for key, tracking in self.quota_tracking.items()
        }
    
    def reset_daily_quota(self):
        """Reseta quota diária (chamado no início de cada dia)"""
        for key in self.quota_tracking:
            self.quota_tracking[key]['used'] = 0
            self.quota_tracking[key]['exceeded'] = False
        self.current_key_index = 0
    
    def has_available_keys(self) -> bool:
        """Verifica se há chaves disponíveis"""
        return any(not tracking['exceeded'] for tracking in self.quota_tracking.values())

