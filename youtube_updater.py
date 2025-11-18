"""
Atualizador de vídeos do YouTube - Atualiza dados de vídeos já existentes na base
"""
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
from typing import List, Optional, Dict
from datetime import datetime
import config
from api_key_manager import APIKeyManager
from models import Video
from supabase_client import SupabaseClient


class YouTubeUpdater:
    """Classe para atualizar vídeos já existentes no banco de dados"""
    
    def __init__(self, api_key_manager: APIKeyManager, supabase_client: SupabaseClient):
        self.api_key_manager = api_key_manager
        self.supabase_client = supabase_client
        self.youtube = None
        self._build_service()
        self.quota_used = 0
        self.quota_tracking = {
            'videos_list': 0,        # 1 quota por chamada (batch de até 50)
        }
    
    def _build_service(self):
        """Constrói serviço do YouTube com chave atual"""
        key = self.api_key_manager.get_current_key()
        if key:
            self.youtube = build('youtube', 'v3', developerKey=key)
        else:
            raise Exception("Nenhuma chave de API disponível")
    
    def _handle_api_error(self, error: HttpError) -> bool:
        """Trata erros da API e rotaciona chave se necessário"""
        if error.resp.status == 403:
            # Quota excedida ou chave inválida
            if self.api_key_manager.handle_quota_error():
                self._build_service()
                return True
            else:
                raise Exception("Todas as chaves de API excederam a quota")
        elif error.resp.status in [500, 503]:
            # Erro temporário do servidor
            time.sleep(2)
            return True
        return False
    
    def _make_request_with_retry(self, request_func, max_retries=3):
        """Executa requisição com retry automático"""
        for attempt in range(max_retries):
            try:
                response = request_func().execute()
                self.quota_used += 1
                self.api_key_manager.add_quota_usage(amount=1)
                self.quota_tracking['videos_list'] += 1
                
                time.sleep(config.REQUEST_DELAY)
                return response
            except HttpError as e:
                if not self._handle_api_error(e):
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Backoff exponencial
                        continue
                    raise
                # Se rotacionou chave, tenta novamente
                if attempt < max_retries - 1:
                    continue
                raise
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise
    
    def get_video_details(self, video_ids: List[str]) -> List[Dict]:
        """
        Obtém detalhes completos de vídeos (batch de até 50)
        
        Args:
            video_ids: Lista de IDs de vídeos (máximo 50)
        
        Returns:
            Lista de dicionários com detalhes dos vídeos
        """
        if not video_ids:
            return []
        
        # Processa em batches de 50
        all_details = []
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i+50]
            
            try:
                request = self.youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=','.join(batch)
                )
                
                response = self._make_request_with_retry(lambda: request)
                
                for item in response.get('items', []):
                    snippet = item.get('snippet', {})
                    statistics = item.get('statistics', {})
                    content_details = item.get('contentDetails', {})
                    
                    all_details.append({
                        'video_id': item['id'],
                        'title': snippet.get('title', ''),
                        'description': snippet.get('description', ''),
                        'published_at': snippet.get('publishedAt', ''),
                        'channel_id': snippet.get('channelId', ''),
                        'views': int(statistics.get('viewCount', 0)),
                        'likes': int(statistics.get('likeCount', 0)),
                        'comments': int(statistics.get('commentCount', 0)),
                        'duration': content_details.get('duration', ''),
                        'tags': snippet.get('tags', []),
                    })
            except Exception as e:
                print(f"Erro ao obter detalhes dos vídeos: {e}")
                continue
        
        return all_details
    
    def has_changes(self, existing_video: Video, updated_data: Dict) -> bool:
        """
        Verifica se há mudanças nos dados do vídeo
        
        Args:
            existing_video: Vídeo existente no banco
            updated_data: Dados atualizados da API
        
        Returns:
            True se há mudanças, False caso contrário
        """
        # Compara apenas views, likes e comments (conforme solicitado)
        if existing_video.views != updated_data.get('views', 0):
            return True
        if existing_video.likes != updated_data.get('likes', 0):
            return True
        if existing_video.comments != updated_data.get('comments', 0):
            return True
        
        return False
    
    def update_video_from_data(self, existing_video: Video, updated_data: Dict) -> Video:
        """
        Cria objeto Video atualizado a partir dos dados da API
        
        Args:
            existing_video: Vídeo existente
            updated_data: Dados atualizados da API
        
        Returns:
            Objeto Video atualizado
        """
        return Video(
            channel_id=existing_video.channel_id,
            video_id=existing_video.video_id,
            title=updated_data.get('title', existing_video.title),
            views=updated_data.get('views', existing_video.views),
            likes=updated_data.get('likes', existing_video.likes),
            comments=updated_data.get('comments', existing_video.comments),
            published_at=updated_data.get('published_at', existing_video.published_at),
            duration=updated_data.get('duration', existing_video.duration),
            video_url=existing_video.video_url,
            tags=updated_data.get('tags', existing_video.tags),
            format=existing_video.format,
            is_short=existing_video.is_short,
            is_invalid=existing_video.is_invalid,
            id=existing_video.id,
            created_at=existing_video.created_at
        )
    
    def update_channel_videos(self, channel_id: str, log_callback=None) -> Dict:
        """
        Atualiza todos os vídeos de um canal
        
        Args:
            channel_id: ID do canal
            log_callback: Função opcional para logs (recebe mensagem e nível)
        
        Returns:
            Dicionário com estatísticas da atualização
        """
        stats = {
            'total': 0,
            'updated': 0,
            'unchanged': 0,
            'errors': 0,
            'not_found': 0
        }
        
        def log(message: str, level: str = "INFO"):
            """Função de log padrão ou callback"""
            if log_callback:
                log_callback(message, level)
            else:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                prefix = {
                    "INFO": "[INFO]",
                    "SUCCESS": "[✓]",
                    "ERROR": "[✗]",
                    "WARNING": "[!]"
                }.get(level, "[INFO]")
                print(f"{timestamp} {prefix} {message}")
        
        try:
            # Busca todos os vídeos do canal no banco
            log(f"Buscando vídeos do canal {channel_id} no banco de dados...")
            existing_videos = self.supabase_client.get_videos_by_channel(channel_id)
            
            if not existing_videos:
                log(f"Nenhum vídeo encontrado no banco para o canal {channel_id}", "WARNING")
                return stats
            
            log(f"Encontrados {len(existing_videos)} vídeos no banco para atualizar (buscados de todas as páginas)")
            
            # Processa vídeos em batches de 50 (limite da API)
            batch_size = 50
            total_batches = (len(existing_videos) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(existing_videos))
                batch_videos = existing_videos[start_idx:end_idx]
                
                log(f"Processando batch {batch_num + 1}/{total_batches} ({len(batch_videos)} vídeos)...")
                
                # Extrai IDs dos vídeos do batch
                video_ids = [v.video_id for v in batch_videos]
                
                # Busca dados atualizados da API
                try:
                    updated_data_list = self.get_video_details(video_ids)
                    updated_data_dict = {d['video_id']: d for d in updated_data_list}
                except Exception as e:
                    log(f"Erro ao buscar dados atualizados do batch: {e}", "ERROR")
                    stats['errors'] += len(batch_videos)
                    continue
                
                # Processa cada vídeo do batch
                for existing_video in batch_videos:
                    stats['total'] += 1
                    
                    # Verifica se o vídeo ainda existe na API
                    if existing_video.video_id not in updated_data_dict:
                        log(f"Vídeo {existing_video.video_id} não encontrado na API (pode ter sido removido)", "WARNING")
                        stats['not_found'] += 1
                        continue
                    
                    updated_data = updated_data_dict[existing_video.video_id]
                    
                    # Valida se o vídeo ainda pertence ao canal correto
                    if updated_data.get('channel_id') != channel_id:
                        log(f"Vídeo {existing_video.video_id} mudou de canal (esperado: {channel_id}, encontrado: {updated_data.get('channel_id')})", "WARNING")
                        stats['errors'] += 1
                        continue
                    
                    # Verifica se há mudanças
                    if self.has_changes(existing_video, updated_data):
                        # Atualiza vídeo
                        updated_video = self.update_video_from_data(existing_video, updated_data)
                        
                        if self.supabase_client.update_video(updated_video):
                            stats['updated'] += 1
                            log(f"  [ATUALIZADO] {existing_video.video_id}: views={updated_video.views} (+{updated_video.views - existing_video.views}), "
                                f"likes={updated_video.likes} (+{updated_video.likes - existing_video.likes}), "
                                f"comments={updated_video.comments} (+{updated_video.comments - existing_video.comments})")
                        else:
                            stats['errors'] += 1
                            log(f"  [ERRO] Falha ao atualizar vídeo {existing_video.video_id}", "ERROR")
                    else:
                        stats['unchanged'] += 1
                        if stats['total'] % 10 == 0:  # Log a cada 10 vídeos sem mudanças
                            log(f"  Processados {stats['total']} vídeos... ({stats['updated']} atualizados, {stats['unchanged']} sem mudanças)")
            
            log(f"Atualização concluída: {stats['updated']} atualizados, {stats['unchanged']} sem mudanças, "
                f"{stats['errors']} erros, {stats['not_found']} não encontrados", "SUCCESS")
            
        except Exception as e:
            log(f"Erro ao atualizar vídeos do canal {channel_id}: {e}", "ERROR")
            import traceback
            traceback.print_exc()
            stats['errors'] += 1
        
        return stats
    
    def update_all_channels_videos(self, channel_ids: List[str], log_callback=None) -> Dict:
        """
        Atualiza vídeos de múltiplos canais
        
        Args:
            channel_ids: Lista de IDs de canais para atualizar
            log_callback: Função opcional para logs
        
        Returns:
            Dicionário com estatísticas totais
        """
        total_stats = {
            'total': 0,
            'updated': 0,
            'unchanged': 0,
            'errors': 0,
            'not_found': 0
        }
        
        def log(message: str, level: str = "INFO"):
            """Função de log padrão ou callback"""
            if log_callback:
                log_callback(message, level)
            else:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                prefix = {
                    "INFO": "[INFO]",
                    "SUCCESS": "[✓]",
                    "ERROR": "[✗]",
                    "WARNING": "[!]"
                }.get(level, "[INFO]")
                print(f"{timestamp} {prefix} {message}")
        
        for i, channel_id in enumerate(channel_ids):
            log("=" * 60)
            log(f"Processando canal {i+1}/{len(channel_ids)}: {channel_id}")
            
            try:
                stats = self.update_channel_videos(channel_id, log_callback=log_callback)
                
                # Acumula estatísticas
                total_stats['total'] += stats['total']
                total_stats['updated'] += stats['updated']
                total_stats['unchanged'] += stats['unchanged']
                total_stats['errors'] += stats['errors']
                total_stats['not_found'] += stats['not_found']
                
            except Exception as e:
                log(f"Erro ao processar canal {channel_id}: {e}", "ERROR")
                import traceback
                traceback.print_exc()
                total_stats['errors'] += 1
        
        return total_stats
    
    def get_quota_info(self) -> dict:
        """Retorna informações de quota usada"""
        total_used = self.quota_used
        quota_limit = config.QUOTA_DAILY_LIMIT
        quota_remaining = max(0, quota_limit - total_used)
        quota_percentage = (total_used / quota_limit * 100) if quota_limit > 0 else 0
        
        return {
            'used': total_used,
            'limit': quota_limit,
            'remaining': quota_remaining,
            'percentage_used': quota_percentage,
            'breakdown': self.quota_tracking.copy()
        }

