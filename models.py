"""
Modelos de dados para canais e vídeos
"""
from datetime import datetime
from typing import Optional, List
import json


class Channel:
    """Modelo de canal do YouTube"""
    
    def __init__(self, channel_id: str, name: str, **kwargs):
        self.id = kwargs.get('id')
        self.channel_id = channel_id
        self.name = name
        self.segment = kwargs.get('segment')
        self.views = kwargs.get('views', 0)
        self.subscribers = kwargs.get('subscribers', 0)
        self.video_count = kwargs.get('video_count', 0)
        self.description = kwargs.get('description')
        self.thumbnail_url = kwargs.get('thumbnail_url')
        self.banner_url = kwargs.get('banner_url')
        self.sponsor_ids = kwargs.get('sponsor_ids', [])
        self.instagram_url = kwargs.get('instagram_url')
        self.tiktok_url = kwargs.get('tiktok_url')
        self.oldest_video_date = kwargs.get('oldest_video_date')
        self.newest_video_date = kwargs.get('newest_video_date')
        self.needs_old_videos = kwargs.get('needs_old_videos', True)  # Default True se não existir no banco
        self.priority = kwargs.get('priority', 5)
        self.stats_history = kwargs.get('stats_history', {})
        self.created_at = kwargs.get('created_at')
        self.updated_at = kwargs.get('updated_at')
    
    def to_dict(self):
        """Converte para dicionário para Supabase"""
        data = {
            'channel_id': self.channel_id,
            'name': self.name,
            'segment': self.segment,
            'views': self.views,
            'subscribers': self.subscribers,
            'video_count': self.video_count,
            'description': self.description,
            'thumbnail_url': self.thumbnail_url,
            'banner_url': self.banner_url,
            'sponsor_ids': json.dumps(self.sponsor_ids) if isinstance(self.sponsor_ids, list) else self.sponsor_ids,
            'instagram_url': self.instagram_url,
            'tiktok_url': self.tiktok_url,
            'oldest_video_date': self.oldest_video_date,
            'newest_video_date': self.newest_video_date,
            # 'needs_old_videos': self.needs_old_videos,  # Comentado se coluna não existir
            # 'priority': self.priority,  # Comentado se coluna não existir
            'stats_history': json.dumps(self.stats_history) if isinstance(self.stats_history, dict) else self.stats_history,
        }
        # Remove None values
        return {k: v for k, v in data.items() if v is not None}
    
    @classmethod
    def from_dict(cls, data: dict):
        """Cria instância a partir de dicionário do Supabase"""
        # Processa sponsor_ids se for string JSON
        sponsor_ids = data.get('sponsor_ids', [])
        if isinstance(sponsor_ids, str):
            try:
                sponsor_ids = json.loads(sponsor_ids)
            except:
                sponsor_ids = []
        
        # Processa stats_history se for string JSON
        stats_history = data.get('stats_history', {})
        if isinstance(stats_history, str):
            try:
                stats_history = json.loads(stats_history)
            except:
                stats_history = {}
        
        return cls(
            channel_id=data['channel_id'],
            name=data['name'],
            id=data.get('id'),
            segment=data.get('segment'),
            views=data.get('views', 0),
            subscribers=data.get('subscribers', 0),
            video_count=data.get('video_count', 0),
            description=data.get('description'),
            thumbnail_url=data.get('thumbnail_url'),
            banner_url=data.get('banner_url'),
            sponsor_ids=sponsor_ids,
            instagram_url=data.get('instagram_url'),
            tiktok_url=data.get('tiktok_url'),
            oldest_video_date=data.get('oldest_video_date'),
            newest_video_date=data.get('newest_video_date'),
            needs_old_videos=data.get('needs_old_videos', True),
            priority=data.get('priority', 5),
            stats_history=stats_history,
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at'),
        )


class Video:
    """Modelo de vídeo do YouTube"""
    
    def __init__(self, channel_id: str, video_id: str, title: str, **kwargs):
        self.id = kwargs.get('id')
        self.channel_id = channel_id
        self.video_id = video_id
        self.title = title
        self.views = kwargs.get('views', 0)
        self.likes = kwargs.get('likes', 0)
        self.comments = kwargs.get('comments', 0)
        self.published_at = kwargs.get('published_at')
        self.duration = kwargs.get('duration')
        self.video_url = kwargs.get('video_url', f"https://www.youtube.com/watch?v={video_id}")
        self.tags = kwargs.get('tags', [])
        self.format = kwargs.get('format', '16:9')
        self.is_short = kwargs.get('is_short', False)
        self.created_at = kwargs.get('created_at')
    
    def to_dict(self):
        """Converte para dicionário para Supabase"""
        data = {
            'channel_id': self.channel_id,
            'video_id': self.video_id,
            'title': self.title,
            'views': self.views,
            'likes': self.likes,
            'comments': self.comments,
            'published_at': self.published_at,
        }
        
        # Campos OBRIGATÓRIOS - sempre incluir (mesmo que vazios)
        # Esses campos são essenciais e devem sempre estar presentes
        
        # video_url: sempre incluir (gerado automaticamente se não fornecido)
        data['video_url'] = self.video_url or f"https://www.youtube.com/watch?v={self.video_id}"
        
        # tags: sempre incluir (mesmo se for lista vazia)
        if isinstance(self.tags, list):
            data['tags'] = json.dumps(self.tags) if self.tags else '[]'
        elif self.tags is not None:
            data['tags'] = self.tags
        else:
            data['tags'] = '[]'
        
        # duration: sempre incluir (salva como vem do YouTube - formato ISO 8601)
        data['duration'] = self.duration if self.duration else None
        
        # format: sempre incluir (tem valor padrão)
        data['format'] = self.format if self.format else '16:9'
        
        # is_short: sempre incluir (tem valor padrão)
        data['is_short'] = self.is_short if self.is_short is not None else False
        
        # Remove apenas campos que são None (mas mantém strings vazias e listas vazias)
        return {k: v for k, v in data.items() if v is not None}
    
    @classmethod
    def from_dict(cls, data: dict):
        """Cria instância a partir de dicionário do Supabase"""
        # Processa tags se for string JSON
        tags = data.get('tags', [])
        if isinstance(tags, str):
            try:
                tags = json.loads(tags)
            except:
                tags = []
        
        return cls(
            channel_id=data['channel_id'],
            video_id=data['video_id'],
            title=data['title'],
            id=data.get('id'),
            views=data.get('views', 0),
            likes=data.get('likes', 0),
            comments=data.get('comments', 0),
            published_at=data.get('published_at'),
            duration=data.get('duration'),
            video_url=data.get('video_url'),
            tags=tags,
            format=data.get('format', '16:9'),
            is_short=data.get('is_short', False),
            created_at=data.get('created_at'),
        )

