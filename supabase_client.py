"""
Cliente para interação com Supabase
"""
from supabase import create_client, Client
from typing import List, Optional
import config
from models import Channel, Video
from datetime import datetime
from utils import parse_datetime


class SupabaseClient:
    """Cliente para operações no Supabase"""
    
    def __init__(self):
        self.client: Client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
    
    def get_channels(self) -> List[Channel]:
        """Busca todos os canais da tabela channels"""
        try:
            response = self.client.table('channels').select('*').execute()
            return [Channel.from_dict(channel) for channel in response.data]
        except Exception as e:
            print(f"Erro ao buscar canais: {e}")
            return []
    
    def get_channels_needing_old_videos(self) -> List[Channel]:
        """Busca canais que ainda precisam buscar vídeos antigos"""
        try:
            # Tenta buscar com a coluna needs_old_videos
            response = self.client.table('channels').select('*').eq('needs_old_videos', True).execute()
            return [Channel.from_dict(channel) for channel in response.data]
        except Exception as e:
            # Se a coluna não existir, busca todos os canais
            # e filtra os que têm oldest_video_date None ou mais recente que 3 meses
            try:
                from datetime import datetime, timedelta
                all_channels = self.get_channels()
                # Filtra canais que precisam de vídeos antigos
                # (aqueles sem oldest_video_date ou com data muito recente)
                three_months_ago = datetime.now() - timedelta(days=90)
                filtered = []
                for channel in all_channels:
                    if channel.oldest_video_date:
                        oldest_dt = parse_datetime(channel.oldest_video_date)
                        if oldest_dt and oldest_dt > three_months_ago:
                            filtered.append(channel)
                    else:
                        # Se não tem oldest_video_date, precisa buscar
                        filtered.append(channel)
                return filtered
            except Exception as e2:
                print(f"Erro ao buscar canais que precisam de vídeos antigos: {e2}")
                # Se tudo falhar, retorna todos os canais
                return self.get_channels()
    
    def get_channel_by_id(self, channel_id: str) -> Optional[Channel]:
        """Busca canal específico por channel_id"""
        try:
            response = self.client.table('channels').select('*').eq('channel_id', channel_id).execute()
            if response.data:
                return Channel.from_dict(response.data[0])
            return None
        except Exception as e:
            print(f"Erro ao buscar canal {channel_id}: {e}")
            return None
    
    def insert_video(self, video: Video) -> bool:
        """Insere vídeo na tabela videos (ignora se já existir)"""
        video_dict = video.to_dict()
        
        # Primeiro, tenta inserir com TODOS os campos (incluindo opcionais)
        # Isso permite usar campos como tags, video_url, format, duration se existirem
        try:
            self.client.table('videos').insert(video_dict).execute()
            return True
        except Exception as e:
            # Se erro for de duplicata, ignora silenciosamente
            error_str = str(e).lower()
            if 'duplicate' in error_str or 'unique' in error_str:
                return False
            
            # Se erro for de tipo incorreto (ex: duration como integer mas recebendo string)
            # Tenta converter duration para segundos
            if 'invalid input syntax for type integer' in error_str and 'duration' in str(e).lower():
                try:
                    # Converte duration de ISO 8601 para segundos
                    from utils import parse_iso8601_duration
                    if 'duration' in video_dict and isinstance(video_dict['duration'], str):
                        duration_str = video_dict['duration']
                        duration_seconds = parse_iso8601_duration(duration_str)
                        video_dict['duration'] = duration_seconds if duration_seconds > 0 else 0
                    
                    self.client.table('videos').insert(video_dict).execute()
                    return True
                except Exception as e3:
                    if 'duplicate' in str(e3).lower() or 'unique' in str(e3).lower():
                        return False
                    print(f"Erro ao inserir vídeo {video.video_id} (com duration convertido): {e3}")
                    # Continua para tentar sem campos opcionais
            
            # Se erro for de coluna não encontrada, tenta sem os campos opcionais
            if 'column' in error_str and 'not found' in error_str:
                try:
                    # Campos obrigatórios apenas
                    required_fields = {
                        'channel_id': video_dict.get('channel_id'),
                        'video_id': video_dict.get('video_id'),
                        'title': video_dict.get('title'),
                        'views': video_dict.get('views', 0),
                        'likes': video_dict.get('likes', 0),
                        'comments': video_dict.get('comments', 0),
                        'published_at': video_dict.get('published_at'),
                    }
                    # Remove None values
                    required_fields = {k: v for k, v in required_fields.items() if v is not None}
                    
                    self.client.table('videos').insert(required_fields).execute()
                    return True
                except Exception as e2:
                    # Se erro for de duplicata, ignora
                    if 'duplicate' in str(e2).lower() or 'unique' in str(e2).lower():
                        return False
                    print(f"Erro ao inserir vídeo {video.video_id} (sem campos opcionais): {e2}")
                    return False
            
            # Outro tipo de erro
            print(f"Erro ao inserir vídeo {video.video_id}: {e}")
            return False
    
    def video_exists(self, video_id: str) -> bool:
        """Verifica se vídeo já existe no banco"""
        try:
            response = self.client.table('videos').select('id').eq('video_id', video_id).execute()
            return len(response.data) > 0
        except Exception as e:
            print(f"Erro ao verificar vídeo {video_id}: {e}")
            return False
    
    def update_channel_stats(self, channel_id: str, views: int, subscribers: int, video_count: int):
        """Atualiza estatísticas do canal"""
        try:
            self.client.table('channels').update({
                'views': views,
                'subscribers': subscribers,
                'video_count': video_count,
                'updated_at': datetime.now().isoformat()
            }).eq('channel_id', channel_id).execute()
        except Exception as e:
            print(f"Erro ao atualizar estatísticas do canal {channel_id}: {e}")
    
    def update_channel_dates(self, channel_id: str, oldest_date: Optional[str] = None, newest_date: Optional[str] = None):
        """Atualiza oldest_video_date e newest_video_date do canal"""
        try:
            update_data = {'updated_at': datetime.now().isoformat()}
            if oldest_date:
                update_data['oldest_video_date'] = oldest_date
            if newest_date:
                update_data['newest_video_date'] = newest_date
            
            self.client.table('channels').update(update_data).eq('channel_id', channel_id).execute()
        except Exception as e:
            print(f"Erro ao atualizar datas do canal {channel_id}: {e}")
    
    def mark_old_videos_complete(self, channel_id: str):
        """Marca que não precisa mais buscar vídeos antigos para o canal"""
        try:
            # Tenta atualizar a coluna needs_old_videos se existir
            self.client.table('channels').update({
                'needs_old_videos': False,
                'updated_at': datetime.now().isoformat()
            }).eq('channel_id', channel_id).execute()
        except Exception as e:
            # Se a coluna não existir, apenas atualiza updated_at
            try:
                self.client.table('channels').update({
                    'updated_at': datetime.now().isoformat()
                }).eq('channel_id', channel_id).execute()
            except Exception as e2:
                print(f"Erro ao marcar vídeos antigos como completos para canal {channel_id}: {e2}")
    
    def reset_old_videos_flag(self, channel_id: str):
        """Reseta flag needs_old_videos para True"""
        try:
            self.client.table('channels').update({
                'needs_old_videos': True,
                'updated_at': datetime.now().isoformat()
            }).eq('channel_id', channel_id).execute()
        except Exception as e:
            print(f"Erro ao resetar flag de vídeos antigos para canal {channel_id}: {e}")
    
    def get_channel_video_dates(self, channel_id: str) -> tuple:
        """Retorna (oldest_video_date, newest_video_date) do canal"""
        try:
            response = self.client.table('channels').select('oldest_video_date,newest_video_date').eq('channel_id', channel_id).execute()
            if response.data:
                data = response.data[0]
                return (data.get('oldest_video_date'), data.get('newest_video_date'))
            return (None, None)
        except Exception as e:
            print(f"Erro ao buscar datas de vídeos do canal {channel_id}: {e}")
            return (None, None)

