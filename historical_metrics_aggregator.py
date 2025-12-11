"""
Módulo para agregação de métricas históricas mensais
"""

from supabase_client import SupabaseClient
from models import Channel, Video
from utils import parse_iso8601_duration, parse_datetime
from datetime import datetime, date
from calendar import monthrange
from typing import Optional, Dict, List
import logging

# Configura logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HistoricalMetricsAggregator:
    """Classe responsável por agregar métricas mensais"""
    
    def __init__(self, supabase_client: SupabaseClient):
        self.client = supabase_client
        self.logger = logger
    
    def is_video_long(self, video: Video) -> Optional[bool]:
        """
        Determina se vídeo é longo ou short
        
        Prioridade:
        1. Campo is_short: se existe e é True → short (False), False → long (True)
        2. Duração: se duration > 180s → long (True), senão → short (False)
        3. Se não tem duração e não tem is_short → None (ignora)
        
        Returns:
            True se for longo, False se for short, None se não puder determinar
        """
        # Ignora vídeos inválidos
        if hasattr(video, 'is_invalid') and video.is_invalid:
            return None
        
        # Verifica campo is_short primeiro
        if hasattr(video, 'is_short') and video.is_short is not None:
            return not video.is_short  # is_short=True → short (False), is_short=False → long (True)
        
        # Se não tem is_short, verifica duração
        if video.duration:
            duration_seconds = parse_iso8601_duration(video.duration)
            if duration_seconds > 0:
                return duration_seconds > 180  # > 180s = long, <= 180s = short
        
        return None  # Sem informação suficiente
    
    def get_monthly_metrics(self, channel_id: str, year: int, month: int) -> Optional[Dict]:
        """
        Busca métricas diárias de um mês específico
        
        Returns:
            Dict com primeira e última métrica do mês, ou None se não houver dados
        """
        try:
            first_day = date(year, month, 1)
            last_day = date(year, month, monthrange(year, month)[1])
            
            # Busca todas as métricas do mês
            response = self.client.client.table('metrics').select('*').eq(
                'channel_id', channel_id
            ).gte('date', first_day.isoformat()).lte(
                'date', last_day.isoformat()
            ).order('date', desc=False).execute()
            
            if not response.data or len(response.data) == 0:
                # Se não tem métricas no mês, busca a mais recente antes do mês
                response_before = self.client.client.table('metrics').select('*').eq(
                    'channel_id', channel_id
                ).lt('date', first_day.isoformat()).order('date', desc=True).limit(1).execute()
                
                if response_before.data:
                    metric = response_before.data[0]
                    return {
                        'first_metric': metric,
                        'last_metric': metric,
                        'first_date': metric.get('date'),
                        'last_date': metric.get('date'),
                        'has_data_in_month': False
                    }
                return None
            
            first_metric = response.data[0]
            last_metric = response.data[-1]
            
            return {
                'first_metric': first_metric,
                'last_metric': last_metric,
                'first_date': first_metric.get('date'),
                'last_date': last_metric.get('date'),
                'has_data_in_month': True
            }
        except Exception as e:
            self.logger.error(f"Erro ao buscar métricas mensais para {channel_id} ({year}/{month}): {e}")
            return None
    
    def get_videos_published_in_month(
        self, 
        channel_id: str, 
        year: int, 
        month: int
    ) -> List[Video]:
        """
        Busca vídeos publicados em um mês específico
        
        Returns:
            Lista de vídeos publicados no mês
        """
        try:
            # Busca todos os vídeos do canal
            all_videos = self.client.get_videos_by_channel(channel_id)
            
            # Filtra vídeos publicados no mês
            videos_in_month = []
            for video in all_videos:
                if video.published_at:
                    try:
                        pub_date_str = video.published_at
                        # Remove 'Z' e converte
                        if pub_date_str.endswith('Z'):
                            pub_date_str = pub_date_str[:-1] + '+00:00'
                        pub_date = datetime.fromisoformat(pub_date_str)
                        if pub_date.year == year and pub_date.month == month:
                            videos_in_month.append(video)
                    except Exception as e:
                        continue
            
            return videos_in_month
        except Exception as e:
            self.logger.error(f"Erro ao buscar vídeos do mês para {channel_id} ({year}/{month}): {e}")
            return []
    
    def aggregate_monthly_metrics(
        self, 
        channel_id: str, 
        year: int, 
        month: int
    ) -> Optional[Dict]:
        """
        Agrega métricas mensais para um canal específico
        
        Returns:
            Dict com as métricas agregadas ou None se não houver dados
        """
        try:
            # 1. Busca métricas diárias do mês
            monthly_metrics = self.get_monthly_metrics(channel_id, year, month)
            
            if not monthly_metrics:
                self.logger.warning(f"Nenhuma métrica encontrada para {channel_id} em {year}/{month}")
                return None
            
            first_metric = monthly_metrics['first_metric']
            last_metric = monthly_metrics['last_metric']
            
            # Calcula valores das métricas
            views = last_metric.get('views', 0)
            subscribers_final = last_metric.get('subscribers', 0)
            subscribers_initial = first_metric.get('subscribers', 0)
            subscribers_diff = subscribers_final - subscribers_initial
            video_count = last_metric.get('video_count', 0)
            
            # 2. Busca vídeos publicados no mês
            videos_in_month = self.get_videos_published_in_month(channel_id, year, month)
            
            # 3. Calcula agregados de vídeos
            longs_posted = 0
            shorts_posted = 0
            longs_views = 0
            shorts_views = 0
            
            for video in videos_in_month:
                views_video = video.views or 0
                is_long = self.is_video_long(video)
                
                if is_long is None:
                    continue  # Ignora vídeos sem informação suficiente
                
                if is_long:
                    longs_posted += 1
                    longs_views += views_video
                else:
                    shorts_posted += 1
                    shorts_views += views_video
            
            return {
                'channel_id': channel_id,
                'year': year,
                'month': month,
                'views': views,
                'subscribers': subscribers_diff,
                'video_count': video_count,
                'longs_posted': longs_posted,
                'shorts_posted': shorts_posted,
                'longs_views': longs_views,
                'shorts_views': shorts_views
            }
        except Exception as e:
            self.logger.error(f"Erro ao agregar métricas para {channel_id} ({year}/{month}): {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def upsert_historical_metric(
        self, 
        channel_id: str, 
        year: int, 
        month: int, 
        metrics: Dict
    ) -> bool:
        """
        Insere ou atualiza registro em historical_metrics usando UPSERT
        
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            metric_data = {
                'channel_id': channel_id,
                'year': year,
                'month': month,
                'views': metrics.get('views', 0),
                'subscribers': metrics.get('subscribers', 0),
                'video_count': metrics.get('video_count', 0),
                'longs_posted': metrics.get('longs_posted', 0),
                'shorts_posted': metrics.get('shorts_posted', 0),
                'longs_views': metrics.get('longs_views', 0),
                'shorts_views': metrics.get('shorts_views', 0),
                'source': 'auto',
                'updated_at': datetime.now().isoformat()
            }
            
            # Usa UPSERT (INSERT ... ON CONFLICT ... UPDATE)
            # O Supabase Python client não suporta diretamente, então fazemos:
            # 1. Tenta inserir
            # 2. Se der erro de duplicata, atualiza
            
            try:
                self.client.client.table('historical_metrics').insert(metric_data).execute()
                return True
            except Exception as e:
                error_str = str(e).lower()
                if 'duplicate' in error_str or 'unique' in error_str or 'constraint' in error_str:
                    # Já existe, atualiza
                    # Remove campos que não devem ser atualizados no UPDATE
                    update_data = {k: v for k, v in metric_data.items() if k not in ['channel_id', 'year', 'month']}
                    self.client.client.table('historical_metrics').update(
                        update_data
                    ).eq('channel_id', channel_id).eq('year', year).eq('month', month).execute()
                    return True
                else:
                    raise
            
        except Exception as e:
            self.logger.error(f"Erro ao fazer UPSERT de historical_metric para {channel_id} ({year}/{month}): {e}")
            return False
    
    def process_current_month(self) -> Dict:
        """
        Processa o mês atual para todos os canais ativos
        
        Returns:
            Dict com estatísticas do processamento
        """
        today = date.today()
        year = today.year
        month = today.month
        
        self.logger.info(f"Processando historical_metrics para {month}/{year}")
        
        # Busca todos os canais
        channels = self.client.get_channels()
        self.logger.info(f"Encontrados {len(channels)} canais para processar")
        
        stats = {
            'channels_processed': 0,
            'channels_updated': 0,
            'channels_created': 0,
            'channels_skipped': 0,
            'errors': 0
        }
        
        for i, channel in enumerate(channels, 1):
            try:
                self.logger.info(f"Processando canal {i}/{len(channels)}: {channel.name} ({channel.channel_id})")
                
                # Agrega métricas do mês atual
                metrics = self.aggregate_monthly_metrics(channel.channel_id, year, month)
                
                if not metrics:
                    self.logger.warning(f"Sem métricas para {channel.name}, pulando...")
                    stats['channels_skipped'] += 1
                    continue
                
                # Verifica se já existe registro
                existing = self.client.client.table('historical_metrics').select('id').eq(
                    'channel_id', channel.channel_id
                ).eq('year', year).eq('month', month).execute()
                
                # Faz UPSERT
                if self.upsert_historical_metric(channel.channel_id, year, month, metrics):
                    if existing.data and len(existing.data) > 0:
                        stats['channels_updated'] += 1
                    else:
                        stats['channels_created'] += 1
                    stats['channels_processed'] += 1
                    self.logger.info(f"✅ {channel.name}: views={metrics['views']:,}, subs={metrics['subscribers']:,}, longs={metrics['longs_posted']}, shorts={metrics['shorts_posted']}")
                else:
                    stats['errors'] += 1
                    self.logger.error(f"❌ Erro ao processar {channel.name}")
                    
            except Exception as e:
                stats['errors'] += 1
                self.logger.error(f"Erro ao processar canal {channel.name}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        self.logger.info(f"Processamento concluído: {stats}")
        return stats
    
    def create_next_month_entries(self) -> Dict:
        """
        Cria entradas para o próximo mês (executado no último dia do mês)
        
        Returns:
            Dict com estatísticas da criação
        """
        today = date.today()
        
        # Calcula próximo mês
        if today.month == 12:
            next_year = today.year + 1
            next_month = 1
        else:
            next_year = today.year
            next_month = today.month + 1
        
        self.logger.info(f"Criando entradas para {next_month}/{next_year}")
        
        # Busca todos os canais
        channels = self.client.get_channels()
        self.logger.info(f"Encontrados {len(channels)} canais")
        
        stats = {
            'entries_created': 0,
            'entries_skipped': 0,
            'errors': 0
        }
        
        for i, channel in enumerate(channels, 1):
            try:
                # Verifica se já existe
                existing = self.client.client.table('historical_metrics').select('id').eq(
                    'channel_id', channel.channel_id
                ).eq('year', next_year).eq('month', next_month).execute()
                
                if existing.data and len(existing.data) > 0:
                    self.logger.debug(f"Entrada já existe para {channel.name} em {next_month}/{next_year}, pulando...")
                    stats['entries_skipped'] += 1
                    continue
                
                # Cria entrada com valores zerados
                metric_data = {
                    'channel_id': channel.channel_id,
                    'year': next_year,
                    'month': next_month,
                    'views': 0,
                    'subscribers': 0,
                    'video_count': 0,
                    'longs_posted': 0,
                    'shorts_posted': 0,
                    'longs_views': 0,
                    'shorts_views': 0,
                    'source': 'auto'
                }
                
                self.client.client.table('historical_metrics').insert(metric_data).execute()
                stats['entries_created'] += 1
                self.logger.info(f"✅ Criada entrada para {channel.name} em {next_month}/{next_year}")
                
            except Exception as e:
                error_str = str(e).lower()
                if 'duplicate' in error_str or 'unique' in error_str:
                    # Já existe (race condition), ignora
                    stats['entries_skipped'] += 1
                else:
                    stats['errors'] += 1
                    self.logger.error(f"Erro ao criar entrada para {channel.name}: {e}")
                continue
        
        self.logger.info(f"Criação de entradas concluída: {stats}")
        return stats

