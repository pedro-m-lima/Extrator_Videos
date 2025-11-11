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
            
            # (Removido: conversão de duration - agora salva como string ISO 8601)
            
            # Se erro for de coluna não encontrada, tenta sem os campos opcionais
            if 'column' in error_str and 'not found' in error_str:
                try:
                    # Campos obrigatórios + duration (se coluna existir)
                    required_fields = {
                        'channel_id': video_dict.get('channel_id'),
                        'video_id': video_dict.get('video_id'),
                        'title': video_dict.get('title'),
                        'views': video_dict.get('views', 0),
                        'likes': video_dict.get('likes', 0),
                        'comments': video_dict.get('comments', 0),
                        'published_at': video_dict.get('published_at'),
                        'duration': video_dict.get('duration'),  # Inclui duration no formato ISO 8601
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
    
    def update_channel_with_history(self, channel_id: str, views: int, subscribers: int, video_count: int, stats_history: Optional[dict] = None):
        """
        Atualiza estatísticas do canal e histórico JSON
        
        Args:
            channel_id: ID do canal
            views: Total de visualizações
            subscribers: Total de inscritos
            video_count: Total de vídeos
            stats_history: Histórico JSON (será atualizado automaticamente se None)
        """
        try:
            import json
            
            # Se stats_history não foi fornecido, busca o existente e atualiza
            if stats_history is None:
                # Busca canal atual
                channel = self.get_channel_by_id(channel_id)
                if channel:
                    # Tenta obter stats_history do banco (se existir)
                    try:
                        response = self.client.table('channels').select('stats_history').eq('channel_id', channel_id).execute()
                        if response.data and response.data[0].get('stats_history'):
                            stats_history_str = response.data[0]['stats_history']
                            if isinstance(stats_history_str, str):
                                stats_history = json.loads(stats_history_str)
                            else:
                                stats_history = stats_history_str
                        else:
                            stats_history = {}
                    except:
                        stats_history = {}
                else:
                    stats_history = {}
            
            # Adiciona dados do dia atual
            now = datetime.now()
            year = str(now.year)
            month = str(now.month).zfill(2)
            day = str(now.day).zfill(2)
            
            if year not in stats_history:
                stats_history[year] = {}
            if month not in stats_history[year]:
                stats_history[year][month] = {}
            if day not in stats_history[year][month]:
                stats_history[year][month][day] = {}
            
            stats_history[year][month][day] = {
                'views': views,
                'subscribers': subscribers,
                'video_count': video_count,
                'timestamp': now.isoformat()
            }
            
            # Atualiza no banco
            update_data = {
                'views': views,
                'subscribers': subscribers,
                'video_count': video_count,
                'stats_history': json.dumps(stats_history),
                'updated_at': datetime.now().isoformat()
            }
            
            self.client.table('channels').update(update_data).eq('channel_id', channel_id).execute()
        except Exception as e:
            print(f"Erro ao atualizar canal {channel_id} com histórico: {e}")
    
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
    
    def get_video_by_id(self, video_id: str) -> Optional[Video]:
        """Busca vídeo existente por video_id"""
        try:
            response = self.client.table('videos').select('*').eq('video_id', video_id).execute()
            if response.data:
                return Video.from_dict(response.data[0])
            return None
        except Exception as e:
            print(f"Erro ao buscar vídeo {video_id}: {e}")
            return None
    
    def update_video(self, video: Video) -> bool:
        """Atualiza vídeo existente na tabela videos"""
        video_dict = video.to_dict()
        
        try:
            # Remove campos que não devem ser atualizados
            update_dict = {k: v for k, v in video_dict.items() if k not in ['id', 'created_at']}
            
            # Remove None values
            update_dict = {k: v for k, v in update_dict.items() if v is not None}
            
            self.client.table('videos').update(update_dict).eq('video_id', video.video_id).execute()
            return True
        except Exception as e:
            error_str = str(e).lower()
            if 'column' in error_str and 'not found' in error_str:
                # Se coluna não existe, tenta atualizar apenas campos básicos
                try:
                    basic_fields = {
                        'title': video_dict.get('title'),
                        'views': video_dict.get('views', 0),
                        'likes': video_dict.get('likes', 0),
                        'comments': video_dict.get('comments', 0),
                        'published_at': video_dict.get('published_at'),
                        'duration': video_dict.get('duration'),  # Inclui duration no formato ISO 8601
                    }
                    basic_fields = {k: v for k, v in basic_fields.items() if v is not None}
                    self.client.table('videos').update(basic_fields).eq('video_id', video.video_id).execute()
                    return True
                except Exception as e2:
                    print(f"Erro ao atualizar vídeo {video.video_id} (campos básicos): {e2}")
                    return False
            print(f"Erro ao atualizar vídeo {video.video_id}: {e}")
            return False

