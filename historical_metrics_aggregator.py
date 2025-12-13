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
        1. Duração: se duration >= 181s → long (True), senão → short (False) - MAIS CONFIÁVEL
        2. Campo is_short: se existe e é True → short (False), False → long (True) - fallback
        3. Formato: se format == "9:16" → short (False), senão → long (True) - fallback adicional
        4. Se não tem nenhuma informação → None (ignora)
        
        Returns:
            True se for longo, False se for short, None se não puder determinar
        """
        # Ignora vídeos inválidos
        if hasattr(video, 'is_invalid') and video.is_invalid:
            return None
        
        # PRIORIDADE 1: Verifica duração primeiro (mais confiável)
        if video.duration:
            duration_seconds = parse_iso8601_duration(video.duration)
            if duration_seconds > 0:
                return duration_seconds >= 181  # >= 181s = long, < 181s = short
        
        # PRIORIDADE 2: Se não tem duração, verifica campo is_short
        if hasattr(video, 'is_short') and video.is_short is not None:
            return not video.is_short  # is_short=True → short (False), is_short=False → long (True)
        
        # PRIORIDADE 3: Se não tem is_short, verifica formato
        if hasattr(video, 'format') and video.format:
            if video.format == "9:16":
                return False  # Formato 9:16 = short
            elif video.format == "16:9":
                return True   # Formato 16:9 = long
        
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
            
            # Busca todas as métricas do mês usando SQL direto
            connection = self.client._get_connection()
            cursor = connection.cursor(dictionary=True)
            
            try:
                query = """
                    SELECT * FROM metrics 
                    WHERE channel_id = %s 
                    AND date >= %s 
                    AND date <= %s 
                    ORDER BY date ASC
                """
                cursor.execute(query, (channel_id, first_day.isoformat(), last_day.isoformat()))
                results = cursor.fetchall()
                
                if not results or len(results) == 0:
                    # Se não tem métricas no mês, busca a mais recente antes do mês
                    query_before = """
                        SELECT * FROM metrics 
                        WHERE channel_id = %s 
                        AND date < %s 
                        ORDER BY date DESC 
                        LIMIT 1
                    """
                    cursor.execute(query_before, (channel_id, first_day.isoformat()))
                    result_before = cursor.fetchone()
                    
                    if result_before:
                        return {
                            'first_metric': result_before,
                            'last_metric': result_before,
                            'first_date': result_before.get('date'),
                            'last_date': result_before.get('date'),
                            'has_data_in_month': False
                        }
                    return None
                
                first_metric = results[0]
                last_metric = results[-1]
                
                return {
                    'first_metric': first_metric,
                    'last_metric': last_metric,
                    'first_date': first_metric.get('date'),
                    'last_date': last_metric.get('date'),
                    'has_data_in_month': True
                }
            finally:
                cursor.close()
                if connection and connection.is_connected():
                    connection.close()
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
            # Busca vídeos diretamente do banco usando SQL (mais eficiente)
            connection = self.client._get_connection()
            cursor = connection.cursor(dictionary=True)
            
            try:
                # Usa SQL para filtrar diretamente por ano e mês
                query = """
                    SELECT * FROM videos 
                    WHERE channel_id = %s 
                    AND YEAR(published_at) = %s 
                    AND MONTH(published_at) = %s
                    ORDER BY published_at ASC
                """
                cursor.execute(query, (channel_id, year, month))
                results = cursor.fetchall()
                
                # Converte para objetos Video
                videos_in_month = [Video.from_dict(row) for row in results]
                
                return videos_in_month
            finally:
                cursor.close()
                if connection and connection.is_connected():
                    connection.close()
            
        except Exception as e:
            self.logger.error(f"Erro ao buscar vídeos do mês para {channel_id} ({year}/{month}): {e}")
            import traceback
            traceback.print_exc()
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
            
            # Inicializa valores padrão
            views = 0
            subscribers_diff = 0
            video_count = 0
            
            if monthly_metrics:
                first_metric = monthly_metrics['first_metric']
                last_metric = monthly_metrics['last_metric']
                
                # Calcula valores das métricas
                views = last_metric.get('views', 0)
                subscribers_final = last_metric.get('subscribers', 0)
                subscribers_initial = first_metric.get('subscribers', 0)
                subscribers_diff = subscribers_final - subscribers_initial
                video_count = last_metric.get('video_count', 0)
            else:
                self.logger.warning(f"Nenhuma métrica diária encontrada para {channel_id} em {year}/{month}, usando apenas dados de vídeos")
            
            # 2. Busca vídeos publicados no mês (sempre calcula, mesmo sem métricas diárias)
            videos_in_month = self.get_videos_published_in_month(channel_id, year, month)
            
            # Se não há vídeos nem métricas, retorna None
            if not videos_in_month and not monthly_metrics:
                self.logger.warning(f"Nenhum dado encontrado para {channel_id} em {year}/{month}")
                return None
            
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
            # Usa INSERT ... ON DUPLICATE KEY UPDATE (MySQL)
            connection = self.client._get_connection()
            cursor = connection.cursor()
            
            try:
                query = """
                    INSERT INTO historical_metrics 
                    (channel_id, year, month, views, subscribers, video_count, 
                     longs_posted, shorts_posted, longs_views, shorts_views, source, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        views = VALUES(views),
                        subscribers = VALUES(subscribers),
                        video_count = VALUES(video_count),
                        longs_posted = VALUES(longs_posted),
                        shorts_posted = VALUES(shorts_posted),
                        longs_views = VALUES(longs_views),
                        shorts_views = VALUES(shorts_views),
                        source = VALUES(source),
                        updated_at = VALUES(updated_at)
                """
                values = (
                    channel_id, year, month,
                    metrics.get('views', 0),
                    metrics.get('subscribers', 0),
                    metrics.get('video_count', 0),
                    metrics.get('longs_posted', 0),
                    metrics.get('shorts_posted', 0),
                    metrics.get('longs_views', 0),
                    metrics.get('shorts_views', 0),
                    'auto',
                    datetime.now().isoformat()
                )
                cursor.execute(query, values)
                connection.commit()
                return True
            finally:
                cursor.close()
                if connection and connection.is_connected():
                    connection.close()
            
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
                connection = self.client._get_connection()
                cursor = connection.cursor(dictionary=True)
                try:
                    query = """
                        SELECT id FROM historical_metrics 
                        WHERE channel_id = %s AND year = %s AND month = %s 
                        LIMIT 1
                    """
                    cursor.execute(query, (channel.channel_id, year, month))
                    existing = cursor.fetchone()
                finally:
                    cursor.close()
                    if connection and connection.is_connected():
                        connection.close()
                
                # Faz UPSERT
                if self.upsert_historical_metric(channel.channel_id, year, month, metrics):
                    if existing:
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
                connection = self.client._get_connection()
                cursor = connection.cursor(dictionary=True)
                try:
                    query = """
                        SELECT id FROM historical_metrics 
                        WHERE channel_id = %s AND year = %s AND month = %s 
                        LIMIT 1
                    """
                    cursor.execute(query, (channel.channel_id, next_year, next_month))
                    existing = cursor.fetchone()
                finally:
                    cursor.close()
                    if connection and connection.is_connected():
                        connection.close()
                
                if existing:
                    self.logger.debug(f"Entrada já existe para {channel.name} em {next_month}/{next_year}, pulando...")
                    stats['entries_skipped'] += 1
                    continue
                
                # Cria entrada com valores zerados usando SQL direto
                connection = self.client._get_connection()
                cursor = connection.cursor()
                try:
                    query = """
                        INSERT INTO historical_metrics 
                        (channel_id, year, month, views, subscribers, video_count, 
                         longs_posted, shorts_posted, longs_views, shorts_views, source)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                        channel.channel_id, next_year, next_month,
                        0, 0, 0, 0, 0, 0, 0, 'auto'
                    )
                    cursor.execute(query, values)
                    connection.commit()
                finally:
                    cursor.close()
                    if connection and connection.is_connected():
                        connection.close()
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

