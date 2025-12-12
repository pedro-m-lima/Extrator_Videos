"""
Cliente para interação com MySQL
"""
import mysql.connector
from mysql.connector import Error, pooling
from typing import List, Optional
import config
from models import Channel, Video
from datetime import datetime
from utils import parse_datetime
import json


class MySQLClient:
    """Cliente para operações no MySQL"""
    
    # Pool de conexões para melhor performance
    _connection_pool = None
    
    @classmethod
    def _get_connection_pool(cls):
        """Cria pool de conexões se não existir"""
        if cls._connection_pool is None:
            try:
                cls._connection_pool = pooling.MySQLConnectionPool(
                    pool_name="youtube_pool",
                    pool_size=5,
                    pool_reset_session=True,
                    host=config.MYSQL_HOST,
                    port=config.MYSQL_PORT,
                    user=config.MYSQL_USER,
                    password=config.MYSQL_PASSWORD,
                    database=config.MYSQL_DATABASE,
                    charset='utf8mb4',
                    collation='utf8mb4_unicode_ci',
                    autocommit=True
                )
            except Error as e:
                print(f"Erro ao criar pool de conexões: {e}")
                raise
        return cls._connection_pool
    
    def _get_connection(self):
        """Obtém conexão do pool"""
        try:
            pool = self._get_connection_pool()
            return pool.get_connection()
        except Error as e:
            print(f"Erro ao obter conexão do pool: {e}")
            # Fallback: conexão direta
            return mysql.connector.connect(
                host=config.MYSQL_HOST,
                port=config.MYSQL_PORT,
                user=config.MYSQL_USER,
                password=config.MYSQL_PASSWORD,
                database=config.MYSQL_DATABASE,
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci',
                autocommit=True
            )
    
    def _execute_query(self, query: str, params: tuple = None, fetch: bool = True):
        """Executa query e retorna resultados"""
        connection = None
        cursor = None
        try:
            connection = self._get_connection()
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query, params or ())
            
            if fetch:
                return cursor.fetchall()
            else:
                connection.commit()
                return cursor.rowcount
        except Error as e:
            if connection:
                connection.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()
    
    def get_channels(self) -> List[Channel]:
        """Busca todos os canais da tabela channels"""
        try:
            query = "SELECT * FROM channels ORDER BY id"
            results = self._execute_query(query)
            return [Channel.from_dict(row) for row in results]
        except Exception as e:
            print(f"Erro ao buscar canais: {e}")
            return []
    
    def get_channels_needing_old_videos(self) -> List[Channel]:
        """Busca canais que ainda precisam buscar vídeos antigos"""
        try:
            # Tenta buscar com a coluna needs_old_videos
            query = "SELECT * FROM channels WHERE needs_old_videos = TRUE ORDER BY id"
            results = self._execute_query(query)
            if results:
                return [Channel.from_dict(row) for row in results]
        except Exception as e:
            # Se a coluna não existir, busca todos os canais e filtra
            pass
        
        try:
            from datetime import datetime, timedelta
            all_channels = self.get_channels()
            three_months_ago = datetime.now() - timedelta(days=90)
            filtered = []
            for channel in all_channels:
                if channel.oldest_video_date:
                    oldest_dt = parse_datetime(channel.oldest_video_date)
                    if oldest_dt and oldest_dt > three_months_ago:
                        filtered.append(channel)
                else:
                    filtered.append(channel)
            return filtered
        except Exception as e2:
            print(f"Erro ao buscar canais que precisam de vídeos antigos: {e2}")
            return self.get_channels()
    
    def get_channel_by_id(self, channel_id: str) -> Optional[Channel]:
        """Busca canal específico por channel_id"""
        try:
            query = "SELECT * FROM channels WHERE channel_id = %s LIMIT 1"
            results = self._execute_query(query, (channel_id,))
            if results:
                return Channel.from_dict(results[0])
            return None
        except Exception as e:
            print(f"Erro ao buscar canal {channel_id}: {e}")
            return None
    
    def insert_video(self, video: Video) -> bool:
        """Insere vídeo na tabela videos (ignora se já existir)"""
        video_dict = video.to_dict()
        
        try:
            # Prepara campos e valores
            fields = []
            values = []
            placeholders = []
            
            for key, value in video_dict.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
                    placeholders.append("%s")
            
            if not fields:
                return False
            
            # Usa INSERT ... ON DUPLICATE KEY UPDATE para evitar duplicatas
            query = f"""
                INSERT INTO videos ({', '.join(fields)})
                VALUES ({', '.join(placeholders)})
                ON DUPLICATE KEY UPDATE
                    {', '.join([f"{f} = VALUES({f})" for f in fields if f != 'video_id'])}
            """
            
            self._execute_query(query, tuple(values), fetch=False)
            return True
        except Exception as e:
            error_str = str(e).lower()
            # Se erro for de duplicata, ignora silenciosamente
            if 'duplicate' in error_str or '1062' in str(e):
                return False
            
            # Se erro for de coluna não encontrada, tenta sem os campos opcionais
            if 'column' in error_str or '1054' in str(e):
                try:
                    required_fields = {
                        'channel_id': video_dict.get('channel_id'),
                        'video_id': video_dict.get('video_id'),
                        'title': video_dict.get('title'),
                        'views': video_dict.get('views', 0),
                        'likes': video_dict.get('likes', 0),
                        'comments': video_dict.get('comments', 0),
                        'published_at': video_dict.get('published_at'),
                    }
                    required_fields = {k: v for k, v in required_fields.items() if v is not None}
                    
                    if required_fields:
                        fields = list(required_fields.keys())
                        values = list(required_fields.values())
                        placeholders = ['%s'] * len(fields)
                        
                        query = f"""
                            INSERT INTO videos ({', '.join(fields)})
                            VALUES ({', '.join(placeholders)})
                            ON DUPLICATE KEY UPDATE
                                {', '.join([f"{f} = VALUES({f})" for f in fields if f != 'video_id'])}
                        """
                        self._execute_query(query, tuple(values), fetch=False)
                        return True
                except Exception as e2:
                    if 'duplicate' in str(e2).lower() or '1062' in str(e2):
                        return False
                    print(f"Erro ao inserir vídeo {video.video_id} (sem campos opcionais): {e2}")
                    return False
            
            print(f"Erro ao inserir vídeo {video.video_id}: {e}")
            return False
    
    def video_exists(self, video_id: str) -> bool:
        """Verifica se vídeo já existe no banco"""
        try:
            query = "SELECT id FROM videos WHERE video_id = %s LIMIT 1"
            results = self._execute_query(query, (video_id,))
            return len(results) > 0
        except Exception as e:
            print(f"Erro ao verificar vídeo {video_id}: {e}")
            return False
    
    def update_channel_stats(self, channel_id: str, views: int, subscribers: int, video_count: int):
        """Atualiza estatísticas do canal"""
        try:
            query = """
                UPDATE channels 
                SET views = %s, subscribers = %s, video_count = %s, updated_at = %s
                WHERE channel_id = %s
            """
            now = datetime.now().isoformat()
            self._execute_query(query, (views, subscribers, video_count, now, channel_id), fetch=False)
        except Exception as e:
            print(f"Erro ao atualizar estatísticas do canal {channel_id}: {e}")
    
    def insert_or_update_metric(self, channel_id: str, views: int, subscribers: int, video_count: int):
        """
        Insere ou atualiza métrica diária na tabela metrics e atualiza estatísticas do canal
        
        Args:
            channel_id: ID do canal
            views: Total de visualizações
            subscribers: Total de inscritos
            video_count: Total de vídeos
        """
        try:
            from datetime import date
            today = date.today().isoformat()
            
            # Usa INSERT ... ON DUPLICATE KEY UPDATE
            query = """
                INSERT INTO metrics (channel_id, date, views, subscribers, video_count)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    views = VALUES(views),
                    subscribers = VALUES(subscribers),
                    video_count = VALUES(video_count)
            """
            self._execute_query(query, (channel_id, today, views, subscribers, video_count), fetch=False)
            
            # Atualiza também as estatísticas atuais na tabela channels
            self.update_channel_stats(channel_id, views, subscribers, video_count)
            
        except Exception as e:
            print(f"Erro ao inserir/atualizar métrica para canal {channel_id}: {e}")
    
    def update_channel_with_history(self, channel_id: str, views: int, subscribers: int, video_count: int, stats_history: Optional[dict] = None):
        """
        [DEPRECADO] Atualiza estatísticas do canal e histórico JSON
        Use insert_or_update_metric() ao invés deste método
        """
        self.insert_or_update_metric(channel_id, views, subscribers, video_count)
    
    def update_channel_dates(self, channel_id: str, oldest_date: Optional[str] = None, newest_date: Optional[str] = None):
        """Atualiza oldest_video_date e newest_video_date do canal"""
        try:
            updates = ['updated_at = %s']
            values = [datetime.now().isoformat()]
            
            if oldest_date:
                updates.append('oldest_video_date = %s')
                values.append(oldest_date)
            if newest_date:
                updates.append('newest_video_date = %s')
                values.append(newest_date)
            
            values.append(channel_id)
            query = f"UPDATE channels SET {', '.join(updates)} WHERE channel_id = %s"
            self._execute_query(query, tuple(values), fetch=False)
        except Exception as e:
            print(f"Erro ao atualizar datas do canal {channel_id}: {e}")
    
    def mark_old_videos_complete(self, channel_id: str):
        """Marca que não precisa mais buscar vídeos antigos para o canal"""
        try:
            # Tenta atualizar a coluna needs_old_videos se existir
            query = """
                UPDATE channels 
                SET needs_old_videos = FALSE, updated_at = %s
                WHERE channel_id = %s
            """
            self._execute_query(query, (datetime.now().isoformat(), channel_id), fetch=False)
        except Exception as e:
            # Se a coluna não existir, apenas atualiza updated_at
            try:
                query = "UPDATE channels SET updated_at = %s WHERE channel_id = %s"
                self._execute_query(query, (datetime.now().isoformat(), channel_id), fetch=False)
            except Exception as e2:
                print(f"Erro ao marcar vídeos antigos como completos para canal {channel_id}: {e2}")
    
    def reset_old_videos_flag(self, channel_id: str):
        """Reseta flag needs_old_videos para True"""
        try:
            query = """
                UPDATE channels 
                SET needs_old_videos = TRUE, updated_at = %s
                WHERE channel_id = %s
            """
            self._execute_query(query, (datetime.now().isoformat(), channel_id), fetch=False)
        except Exception as e:
            print(f"Erro ao resetar flag de vídeos antigos para canal {channel_id}: {e}")
    
    def get_channel_video_dates(self, channel_id: str) -> tuple:
        """Retorna (oldest_video_date, newest_video_date) do canal"""
        try:
            query = "SELECT oldest_video_date, newest_video_date FROM channels WHERE channel_id = %s LIMIT 1"
            results = self._execute_query(query, (channel_id,))
            if results:
                data = results[0]
                return (data.get('oldest_video_date'), data.get('newest_video_date'))
            return (None, None)
        except Exception as e:
            print(f"Erro ao buscar datas de vídeos do canal {channel_id}: {e}")
            return (None, None)
    
    def get_video_by_id(self, video_id: str) -> Optional[Video]:
        """Busca vídeo existente por video_id"""
        try:
            query = "SELECT * FROM videos WHERE video_id = %s LIMIT 1"
            results = self._execute_query(query, (video_id,))
            if results:
                return Video.from_dict(results[0])
            return None
        except Exception as e:
            print(f"Erro ao buscar vídeo {video_id}: {e}")
            return None
    
    def update_video(self, video: Video) -> bool:
        """Atualiza vídeo existente na tabela videos"""
        video_dict = video.to_dict()
        
        try:
            # Remove campos que não devem ser atualizados
            update_dict = {k: v for k, v in video_dict.items() if k not in ['id', 'created_at', 'video_id'] and v is not None}
            
            if not update_dict:
                return False
            
            # Prepara UPDATE
            updates = []
            values = []
            for key, value in update_dict.items():
                updates.append(f"{key} = %s")
                values.append(value)
            
            values.append(video.video_id)
            query = f"UPDATE videos SET {', '.join(updates)} WHERE video_id = %s"
            self._execute_query(query, tuple(values), fetch=False)
            return True
        except Exception as e:
            error_str = str(e).lower()
            if 'column' in error_str or '1054' in str(e):
                # Se coluna não existe, tenta atualizar apenas campos básicos
                try:
                    basic_fields = {
                        'title': video_dict.get('title'),
                        'views': video_dict.get('views', 0),
                        'likes': video_dict.get('likes', 0),
                        'comments': video_dict.get('comments', 0),
                        'published_at': video_dict.get('published_at'),
                    }
                    basic_fields = {k: v for k, v in basic_fields.items() if v is not None}
                    
                    if basic_fields:
                        updates = []
                        values = []
                        for key, value in basic_fields.items():
                            updates.append(f"{key} = %s")
                            values.append(value)
                        values.append(video.video_id)
                        query = f"UPDATE videos SET {', '.join(updates)} WHERE video_id = %s"
                        self._execute_query(query, tuple(values), fetch=False)
                        return True
                except Exception as e2:
                    print(f"Erro ao atualizar vídeo {video.video_id} (campos básicos): {e2}")
                    return False
            print(f"Erro ao atualizar vídeo {video.video_id}: {e}")
            return False
    
    def get_videos_by_channel(self, channel_id: str) -> List[Video]:
        """
        Busca todos os vídeos de um canal específico (com paginação completa)
        Garante cobertura de todos os vídeos, mesmo canais com mais de 20 mil vídeos
        
        Args:
            channel_id: ID do canal
        
        Returns:
            Lista completa de todos os vídeos do canal
        """
        all_videos = []
        page_size = 1000  # Tamanho da página para MySQL
        offset = 0
        page_num = 0
        
        try:
            print(f"Buscando vídeos do canal {channel_id} com paginação...")
            
            while True:
                page_num += 1
                
                query = "SELECT * FROM videos WHERE channel_id = %s ORDER BY id LIMIT %s OFFSET %s"
                results = self._execute_query(query, (channel_id, page_size, offset))
                
                if not results or len(results) == 0:
                    print(f"  Página {page_num}: Nenhum dado retornado. Total de vídeos encontrados: {len(all_videos)}")
                    break
                
                page_videos = [Video.from_dict(row) for row in results]
                all_videos.extend(page_videos)
                
                print(f"  Página {page_num}: {len(page_videos)} vídeos (total acumulado: {len(all_videos)})")
                
                # Se retornou menos que o tamanho da página, chegou ao fim
                if len(results) < page_size:
                    print(f"  Última página completa. Total final: {len(all_videos)} vídeos")
                    break
                
                # Próxima página
                offset += page_size
                
                # Proteção contra loop infinito
                if page_num > 1000:
                    print(f"  AVISO: Limite de páginas atingido (1000). Total coletado: {len(all_videos)} vídeos")
                    break
            
            print(f"Busca completa: {len(all_videos)} vídeos encontrados em {page_num} página(s)")
            return all_videos
            
        except Exception as e:
            print(f"Erro ao buscar vídeos do canal {channel_id} na página {page_num}: {e}")
            print(f"Vídeos coletados até o erro: {len(all_videos)}")
            import traceback
            traceback.print_exc()
            return all_videos
    
    def get_all_videos(self, limit: Optional[int] = None) -> List[Video]:
        """
        Busca todos os vídeos do banco com paginação completa
        
        Args:
            limit: Número máximo de vídeos a retornar (None = todos)
        
        Returns:
            Lista de todos os vídeos
        """
        all_videos = []
        page_size = 1000
        offset = 0
        page_num = 0
        
        try:
            print(f"Buscando todos os vídeos do banco com paginação...")
            
            while True:
                page_num += 1
                
                if limit and len(all_videos) >= limit:
                    all_videos = all_videos[:limit]
                    print(f"  Limite atingido: {limit} vídeos")
                    break
                
                query = "SELECT * FROM videos ORDER BY id LIMIT %s OFFSET %s"
                results = self._execute_query(query, (page_size, offset))
                
                if not results or len(results) == 0:
                    print(f"  Página {page_num}: Nenhum dado retornado. Total de vídeos encontrados: {len(all_videos)}")
                    break
                
                page_videos = [Video.from_dict(row) for row in results]
                all_videos.extend(page_videos)
                
                print(f"  Página {page_num}: {len(page_videos)} vídeos (total acumulado: {len(all_videos)})")
                
                if len(results) < page_size:
                    print(f"  Última página completa. Total final: {len(all_videos)} vídeos")
                    break
                
                offset += page_size
                
                if page_num > 1000:
                    print(f"  AVISO: Limite de páginas atingido (1000). Total coletado: {len(all_videos)} vídeos")
                    break
            
            print(f"Busca completa: {len(all_videos)} vídeos encontrados em {page_num} página(s)")
            return all_videos
            
        except Exception as e:
            print(f"Erro ao buscar todos os vídeos na página {page_num}: {e}")
            print(f"Vídeos coletados até o erro: {len(all_videos)}")
            import traceback
            traceback.print_exc()
            return all_videos
    
    # Métodos adicionais para compatibilidade com código que acessa client.client.table()
    @property
    def client(self):
        """Propriedade para compatibilidade com código antigo que usa client.client.table()"""
        return self


# Alias para compatibilidade - permite usar SupabaseClient = MySQLClient
SupabaseClient = MySQLClient

