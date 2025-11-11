"""
Extrator de vídeos do YouTube usando API v3
"""
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
from typing import List, Optional, Dict, Tuple
from datetime import datetime, timedelta
import config
from api_key_manager import APIKeyManager
from models import Video
from utils import detect_short, parse_datetime, format_datetime, get_date_before


class YouTubeExtractor:
    """Classe para extrair vídeos do YouTube"""
    
    def __init__(self, api_key_manager: APIKeyManager):
        self.api_key_manager = api_key_manager
        self.youtube = None
        self._build_service()
        self.quota_used = 0
        self.quota_tracking = {
            'channels_list': 0,      # 1 quota por chamada
            'playlist_items': 0,     # 1 quota por chamada
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
    
    def _make_request_with_retry(self, request_func, max_retries=3, request_type='general'):
        """Executa requisição com retry automático"""
        for attempt in range(max_retries):
            try:
                response = request_func().execute()
                self.quota_used += 1
                self.api_key_manager.add_quota_usage(amount=1)
                
                # Rastreia por tipo de requisição
                if request_type == 'channels_list':
                    self.quota_tracking['channels_list'] += 1
                elif request_type == 'playlist_items':
                    self.quota_tracking['playlist_items'] += 1
                elif request_type == 'videos_list':
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
    
    def get_upload_playlist_id(self, channel_id: str) -> Optional[str]:
        """Obtém ID da playlist de uploads do canal"""
        try:
            request = self.youtube.channels().list(
                part='contentDetails',
                id=channel_id
            )
            response = self._make_request_with_retry(lambda: request, request_type='channels_list')
            
            if response.get('items'):
                uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                return uploads_playlist_id
            return None
        except Exception as e:
            print(f"Erro ao obter playlist de uploads do canal {channel_id}: {e}")
            return None
    
    def get_all_videos_from_playlist(self, playlist_id: str, start_date: Optional[str] = None) -> List[Dict]:
        """
        Busca TODOS os vídeos da playlist (sem limite)
        
        Args:
            playlist_id: ID da playlist de uploads
            start_date: Data de início (opcional, filtra vídeos mais antigos que esta data)
        
        Returns:
            Lista completa de vídeos encontrados
        """
        videos = []
        next_page_token = None
        target_date = parse_datetime(start_date) if start_date else None
        
        try:
            # Navega por TODAS as páginas até não haver mais vídeos
            while True:
                request = self.youtube.playlistItems().list(
                    part='snippet,contentDetails',
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                
                response = self._make_request_with_retry(lambda: request, request_type='playlist_items')
                
                items = response.get('items', [])
                if not items:
                    break
                
                # Processa todos os itens da página
                for item in items:
                    snippet = item.get('snippet', {})
                    published_at = snippet.get('publishedAt')
                    
                    if published_at:
                        published_dt = parse_datetime(published_at)
                        if published_dt:
                            # Se tem target_date, filtra apenas vídeos mais antigos
                            if target_date:
                                if published_dt.tzinfo is None:
                                    from datetime import timezone
                                    published_dt = published_dt.replace(tzinfo=timezone.utc)
                                if target_date.tzinfo is None:
                                    from datetime import timezone
                                    target_date = target_date.replace(tzinfo=timezone.utc)
                                
                                if published_dt >= target_date:
                                    continue  # Pula vídeos mais recentes que target_date
                            
                            video_id = snippet.get('resourceId', {}).get('videoId')
                            if video_id:
                                videos.append({
                                    'video_id': video_id,
                                    'title': snippet.get('title', ''),
                                    'published_at': published_at,
                                    'description': snippet.get('description', ''),
                                    'channel_id': snippet.get('channelId', ''),
                                })
                
                # Continua para próxima página
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
            
            return videos
        except Exception as e:
            print(f"Erro ao buscar todos os vídeos: {e}")
            return videos
    
    def get_old_videos_retroactive(self, playlist_id: str, start_date: Optional[str], max_videos: int = 50) -> List[Dict]:
        """
        Busca vídeos antigos retroativamente (do mais recente para o mais antigo)
        
        A API retorna vídeos ordenados do mais recente para o mais antigo.
        Navegamos pelas páginas até encontrar vídeos mais antigos que a data inicial.
        
        Args:
            playlist_id: ID da playlist de uploads
            start_date: Data de início (oldest_video_date se existir, senão newest_video_date)
            max_videos: Número máximo de vídeos a buscar
        
        Returns:
            Lista de vídeos encontrados (ordenados do mais recente para o mais antigo)
        """
        videos = []
        next_page_token = None
        target_date = parse_datetime(start_date) if start_date else None
        
        # Se não tem data inicial, busca os vídeos mais recentes primeiro
        # (primeira busca do canal)
        if not target_date:
            from datetime import timezone
            # Usa data atual como limite - busca vídeos mais antigos que agora
            target_date = datetime.now(timezone.utc)
        
        try:
            # Navega pelas páginas da playlist (que retorna do mais recente para o mais antigo)
            # Se não tem target_date, pega os primeiros vídeos da lista (mais recentes)
            while len(videos) < max_videos:
                request = self.youtube.playlistItems().list(
                    part='snippet,contentDetails',
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                
                response = self._make_request_with_retry(lambda: request, request_type='playlist_items')
                
                items = response.get('items', [])
                if not items:
                    break
                
                # Processa itens da página atual
                # A API retorna do mais recente para o mais antigo
                for item in items:
                    if len(videos) >= max_videos:
                        break
                    
                    snippet = item.get('snippet', {})
                    published_at = snippet.get('publishedAt')
                    
                    if published_at:
                        published_dt = parse_datetime(published_at)
                        if published_dt:
                            # Garante que ambos têm timezone para comparação
                            if published_dt.tzinfo is None:
                                from datetime import timezone
                                published_dt = published_dt.replace(tzinfo=timezone.utc)
                            if target_date.tzinfo is None:
                                from datetime import timezone
                                target_date = target_date.replace(tzinfo=timezone.utc)
                            
                            # Se o vídeo é mais antigo que a data alvo, adiciona
                            # (ou se não tem target_date definido, adiciona todos)
                            if published_dt < target_date:
                                video_id = snippet.get('resourceId', {}).get('videoId')
                                if video_id:
                                    videos.append({
                                        'video_id': video_id,
                                        'title': snippet.get('title', ''),
                                        'published_at': published_at,
                                        'description': snippet.get('description', ''),
                                        'channel_id': snippet.get('channelId', ''),
                                    })
                                    
                                    if len(videos) >= max_videos:
                                        break
                
                # Se coletou vídeos suficientes, para
                if len(videos) >= max_videos:
                    break
                
                # Continua para próxima página
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
            
            # Retorna vídeos ordenados do mais recente para o mais antigo
            return videos[:max_videos]
        except Exception as e:
            print(f"Erro ao buscar vídeos antigos: {e}")
            return videos
    
    def get_new_videos(self, playlist_id: str, since_date: Optional[str]) -> List[Dict]:
        """
        Busca vídeos novos publicados após uma data
        
        Args:
            playlist_id: ID da playlist de uploads
            since_date: Data a partir da qual buscar (newest_video_date)
        
        Returns:
            Lista de vídeos encontrados
        """
        videos = []
        next_page_token = None
        
        # Se não tem data, busca todos os vídeos recentes
        published_after = None
        if since_date:
            published_dt = parse_datetime(since_date)
            if published_dt:
                published_after = published_dt.isoformat() + 'Z'
        
        try:
            while True:
                request = self.youtube.playlistItems().list(
                    part='snippet,contentDetails',
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                
                response = self._make_request_with_retry(lambda: request, request_type='playlist_items')
                
                items = response.get('items', [])
                if not items:
                    break
                
                for item in items:
                    snippet = item.get('snippet', {})
                    published_at = snippet.get('publishedAt')
                    
                    if published_at:
                        published_dt = parse_datetime(published_at)
                        # Se tem data limite, filtra
                        if published_after:
                            since_dt = parse_datetime(since_date)
                            if published_dt and since_dt and published_dt <= since_dt:
                                # Já passou da data limite, para busca
                                return videos
                        
                        video_id = snippet.get('resourceId', {}).get('videoId')
                        if video_id:
                            videos.append({
                                'video_id': video_id,
                                'title': snippet.get('title', ''),
                                'published_at': published_at,
                                'description': snippet.get('description', ''),
                                'channel_id': snippet.get('channelId', ''),
                            })
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
            
            return videos
        except Exception as e:
            print(f"Erro ao buscar vídeos novos: {e}")
            return videos
    
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
                
                response = self._make_request_with_retry(lambda: request, request_type='videos_list')
                
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
    
    def process_videos(self, video_data_list: List[Dict], channel_id: str) -> List[Video]:
        """
        Processa lista de vídeos e retorna objetos Video
        
        Args:
            video_data_list: Lista de dicionários com dados básicos dos vídeos
            channel_id: ID do canal (OBRIGATÓRIO - usado para validar e garantir consistência)
        
        Returns:
            Lista de objetos Video
        """
        if not video_data_list:
            return []
        
        # PRIMEIRO: Filtra vídeos que pertencem ao canal correto
        # Remove vídeos que têm channel_id diferente do esperado
        filtered_videos = []
        for video_data in video_data_list:
            video_channel_id = video_data.get('channel_id', '')
            if video_channel_id and video_channel_id != channel_id:
                print(f"AVISO: Vídeo {video_data.get('video_id', '?')} pertence ao canal {video_channel_id}, mas está sendo processado para o canal {channel_id}. Ignorando.")
                continue
            filtered_videos.append(video_data)
        
        if not filtered_videos:
            print(f"Nenhum vídeo válido encontrado para o canal {channel_id}")
            return []
        
        # Extrai IDs dos vídeos filtrados
        video_ids = [v['video_id'] for v in filtered_videos]
        
        # Obtém detalhes completos
        details = self.get_video_details(video_ids)
        
        # Cria dicionário de detalhes por video_id
        details_dict = {d['video_id']: d for d in details}
        
        videos = []
        for video_data in filtered_videos:
            video_id = video_data['video_id']
            details = details_dict.get(video_id, {})
            
            # VALIDAÇÃO CRÍTICA: Verifica se o channel_id dos detalhes corresponde
            details_channel_id = details.get('channel_id', '')
            if details_channel_id and details_channel_id != channel_id:
                print(f"ERRO: Vídeo {video_id} tem channel_id={details_channel_id} nos detalhes, mas esperado {channel_id}. Ignorando vídeo.")
                continue
            
            # Detecta se é Short
            format_type, is_short = detect_short(
                details.get('duration', ''),
                details.get('title', video_data.get('title', '')),
                details.get('description', video_data.get('description', ''))
            )
            
            # USA SEMPRE o channel_id passado como parâmetro, não o da API
            video = Video(
                channel_id=channel_id,  # SEMPRE usa o channel_id do parâmetro
                video_id=video_id,
                title=details.get('title', video_data.get('title', '')),
                views=details.get('views', 0),
                likes=details.get('likes', 0),
                comments=details.get('comments', 0),
                published_at=details.get('published_at', video_data.get('published_at', '')),
                duration=details.get('duration', ''),
                video_url=f"https://www.youtube.com/watch?v={video_id}",
                tags=details.get('tags', []),
                format=format_type,
                is_short=is_short
            )
            
            videos.append(video)
        
        return videos

