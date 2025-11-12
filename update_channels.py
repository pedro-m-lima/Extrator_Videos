"""
Script para atualizar estatísticas de todos os canais diariamente
Versão otimizada com checkpoint, processamento paralelo e tratamento robusto de erros
"""
import os
import sys
import json
import time
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError, as_completed
from typing import List, Dict, Optional, Set
import config
from api_key_manager import APIKeyManager
from youtube_extractor import YouTubeExtractor
from supabase_client import SupabaseClient
from models import Channel


class CheckpointManager:
    """Gerencia checkpoint para permitir retomar processamento"""
    
    def __init__(self, checkpoint_file: str):
        self.checkpoint_file = checkpoint_file
        self.checkpoint_data = self._load_checkpoint()
    
    def _load_checkpoint(self) -> Dict:
        """Carrega checkpoint do arquivo"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Verifica se é do dia atual
                    checkpoint_date = data.get('date')
                    if checkpoint_date == date.today().isoformat():
                        return data
            except Exception as e:
                print(f"Erro ao carregar checkpoint: {e}")
        return {
            'date': date.today().isoformat(),
            'processed_channels': [],
            'failed_channels': [],
            'stats': {
                'total': 0,
                'success': 0,
                'errors': 0,
                'start_time': datetime.now().isoformat()
            }
        }
    
    def save_checkpoint(self):
        """Salva checkpoint no arquivo"""
        try:
            self.checkpoint_data['stats']['last_update'] = datetime.now().isoformat()
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(self.checkpoint_data, f, indent=2)
        except Exception as e:
            print(f"Erro ao salvar checkpoint: {e}")
    
    def is_processed(self, channel_id: str) -> bool:
        """Verifica se canal já foi processado"""
        return channel_id in self.checkpoint_data['processed_channels']
    
    def mark_processed(self, channel_id: str):
        """Marca canal como processado"""
        if channel_id not in self.checkpoint_data['processed_channels']:
            self.checkpoint_data['processed_channels'].append(channel_id)
            self.checkpoint_data['stats']['success'] += 1
    
    def mark_failed(self, channel_id: str, error: str):
        """Marca canal como falhado"""
        if channel_id not in self.checkpoint_data['failed_channels']:
            self.checkpoint_data['failed_channels'].append({
                'channel_id': channel_id,
                'error': str(error),
                'timestamp': datetime.now().isoformat()
            })
            self.checkpoint_data['stats']['errors'] += 1
    
    def get_processed_channels(self) -> Set[str]:
        """Retorna conjunto de canais já processados"""
        return set(self.checkpoint_data['processed_channels'])
    
    def clear_checkpoint(self):
        """Limpa checkpoint (para novo dia)"""
        self.checkpoint_data = {
            'date': date.today().isoformat(),
            'processed_channels': [],
            'failed_channels': [],
            'stats': {
                'total': 0,
                'success': 0,
                'errors': 0,
                'start_time': datetime.now().isoformat()
            }
        }
        self.save_checkpoint()


def log(message: str, level: str = "INFO"):
    """Log formatado"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = {
        "INFO": "ℹ",
        "SUCCESS": "✓",
        "WARNING": "⚠",
        "ERROR": "✗",
        "DEBUG": "🔍"
    }.get(level, "•")
    print(f"{timestamp} [{level}] {prefix} {message}")


def process_single_channel(
    channel: Channel,
    youtube_extractor: YouTubeExtractor,
    supabase_client: SupabaseClient,
    checkpoint_manager: CheckpointManager,
    timeout: int = config.CHANNEL_TIMEOUT
) -> Dict:
    """
    Processa um único canal com timeout e retry
    
    Returns:
        Dict com resultado: {'success': bool, 'channel_id': str, 'error': str, 'stats': dict}
    """
    channel_id = channel.channel_id
    channel_name = channel.name
    
    # Verifica se já foi processado hoje
    if checkpoint_manager.is_processed(channel_id):
        return {
            'success': True,
            'channel_id': channel_id,
            'skipped': True,
            'message': 'Já processado hoje'
        }
    
    try:
        # Busca estatísticas com timeout
        stats = None
        start_time = time.time()
        
        # Retry com timeout
        max_retries = config.RETRY_MAX_ATTEMPTS
        for attempt in range(max_retries):
            try:
                stats = youtube_extractor.get_channel_statistics(channel_id)
                if stats:
                    break
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = config.RETRY_DELAY_BASE * (2 ** attempt)
                    log(f"  Tentativa {attempt + 1}/{max_retries} falhou para {channel_name}, aguardando {wait_time}s...", "WARNING")
                    time.sleep(wait_time)
                else:
                    raise
        
        elapsed_time = time.time() - start_time
        
        if not stats:
            error_msg = f"Não foi possível obter estatísticas do canal {channel_name}"
            checkpoint_manager.mark_failed(channel_id, error_msg)
            return {
                'success': False,
                'channel_id': channel_id,
                'error': error_msg
            }
        
        # Verifica timeout
        if elapsed_time > timeout:
            error_msg = f"Timeout ao processar canal {channel_name} ({elapsed_time:.1f}s > {timeout}s)"
            checkpoint_manager.mark_failed(channel_id, error_msg)
            return {
                'success': False,
                'channel_id': channel_id,
                'error': error_msg
            }
        
        # Insere ou atualiza métrica diária na tabela metrics
        supabase_client.insert_or_update_metric(
            channel_id=channel_id,
            views=stats['views'],
            subscribers=stats['subscribers'],
            video_count=stats['video_count']
        )
        
        # Marca como processado
        checkpoint_manager.mark_processed(channel_id)
        
        # Rate limiting
        time.sleep(config.RATE_LIMIT_DELAY)
        
        return {
            'success': True,
            'channel_id': channel_id,
            'stats': stats,
            'elapsed_time': elapsed_time
        }
        
    except Exception as e:
        error_msg = f"Erro ao processar canal {channel_name}: {str(e)}"
        checkpoint_manager.mark_failed(channel_id, error_msg)
        log(f"  {error_msg}", "ERROR")
        return {
            'success': False,
            'channel_id': channel_id,
            'error': error_msg
        }


def check_quota(youtube_extractor: YouTubeExtractor) -> bool:
    """Verifica se há quota suficiente para continuar"""
    quota_info = youtube_extractor.get_quota_info()
    remaining = quota_info['remaining']
    
    if remaining < config.QUOTA_STOP_THRESHOLD:
        log(f"Quota muito baixa ({remaining}), parando processamento", "WARNING")
        return False
    
    if remaining < config.QUOTA_WARNING_THRESHOLD:
        log(f"Quota baixa ({remaining}), continuando com cuidado", "WARNING")
    
    return True


def update_all_channels(channel_ids: Optional[List[str]] = None):
    """
    Atualiza estatísticas de canais com processamento paralelo e checkpoint
    
    Args:
        channel_ids: Lista de IDs de canais para atualizar. Se None, atualiza todos os canais.
    """
    start_time = datetime.now()
    checkpoint_manager = CheckpointManager(config.CHECKPOINT_FILE)
    
    try:
        # Inicializa clientes
        log("Inicializando clientes...")
        api_key_manager = APIKeyManager()
        youtube_extractor = YouTubeExtractor(api_key_manager)
        supabase_client = SupabaseClient()
        
        # Verifica quota inicial
        if not check_quota(youtube_extractor):
            log("Quota insuficiente para iniciar processamento", "ERROR")
            return
        
        # Busca canais
        if channel_ids:
            log(f"Buscando {len(channel_ids)} canal(is) específico(s)...")
            all_channels = []
            for channel_id in channel_ids:
                channel = supabase_client.get_channel_by_id(channel_id.strip())
                if channel:
                    all_channels.append(channel)
                else:
                    log(f"Canal não encontrado: {channel_id}", "WARNING")
            
            if not all_channels:
                log("Nenhum canal válido encontrado", "WARNING")
                return
        else:
            log("Buscando todos os canais...")
            all_channels = supabase_client.get_channels()
        
        if not all_channels:
            log("Nenhum canal encontrado", "WARNING")
            return
        
        # Filtra canais já processados hoje
        processed_ids = checkpoint_manager.get_processed_channels()
        channels_to_process = [ch for ch in all_channels if ch.channel_id not in processed_ids]
        
        total_channels = len(all_channels)
        remaining_channels = len(channels_to_process)
        
        log(f"Total de canais: {total_channels}")
        log(f"Canais já processados hoje: {len(processed_ids)}")
        log(f"Canais a processar: {remaining_channels}")
        
        if remaining_channels == 0:
            log("Todos os canais já foram processados hoje!", "SUCCESS")
            return
        
        # Atualiza estatísticas do checkpoint
        checkpoint_manager.checkpoint_data['stats']['total'] = total_channels
        
        # Divide em batches
        batch_size = config.BATCH_SIZE
        total_batches = (remaining_channels + batch_size - 1) // batch_size
        
        log(f"Processando em {total_batches} lotes de até {batch_size} canais")
        log(f"Processamento paralelo: {config.MAX_CONCURRENT_CHANNELS} canais simultâneos")
        
        total_updated = 0
        total_errors = 0
        processed_count = 0
        
        # Processa em batches
        for batch_num in range(total_batches):
            batch_start = batch_num * batch_size
            batch_end = min(batch_start + batch_size, remaining_channels)
            batch = channels_to_process[batch_start:batch_end]
            
            log(f"\n{'='*60}")
            log(f"Lote {batch_num + 1}/{total_batches} ({len(batch)} canais)")
            log(f"{'='*60}")
            
            # Verifica quota antes de processar lote
            if not check_quota(youtube_extractor):
                log("Parando processamento devido à quota baixa", "WARNING")
                break
            
            # Processa batch em paralelo
            batch_results = []
            with ThreadPoolExecutor(max_workers=config.MAX_CONCURRENT_CHANNELS) as executor:
                # Submete todas as tarefas
                future_to_channel = {
                    executor.submit(
                        process_single_channel,
                        channel,
                        youtube_extractor,
                        supabase_client,
                        checkpoint_manager,
                        config.CHANNEL_TIMEOUT
                    ): channel
                    for channel in batch
                }
                
                # Processa resultados conforme completam
                for future in as_completed(future_to_channel):
                    channel = future_to_channel[future]
                    try:
                        result = future.result(timeout=config.CHANNEL_TIMEOUT + 5)
                        batch_results.append(result)
                        processed_count += 1
                        
                        if result.get('success'):
                            if not result.get('skipped'):
                                total_updated += 1
                                stats = result.get('stats', {})
                                elapsed = result.get('elapsed_time', 0)
                                log(
                                    f"[{processed_count}/{remaining_channels}] ✓ {channel.name}: "
                                    f"{stats.get('views', 0):,} views, "
                                    f"{stats.get('subscribers', 0):,} inscritos, "
                                    f"{stats.get('video_count', 0)} vídeos "
                                    f"({elapsed:.1f}s)",
                                    "SUCCESS"
                                )
                            else:
                                log(f"[{processed_count}/{remaining_channels}] ⊘ {channel.name}: {result.get('message')}", "INFO")
                        else:
                            total_errors += 1
                            log(f"[{processed_count}/{remaining_channels}] ✗ {channel.name}: {result.get('error', 'Erro desconhecido')}", "ERROR")
                            
                    except FutureTimeoutError:
                        total_errors += 1
                        error_msg = f"Timeout ao processar {channel.name}"
                        checkpoint_manager.mark_failed(channel.channel_id, error_msg)
                        log(f"[{processed_count}/{remaining_channels}] ✗ {channel.name}: {error_msg}", "ERROR")
                    except Exception as e:
                        total_errors += 1
                        error_msg = f"Erro inesperado: {str(e)}"
                        checkpoint_manager.mark_failed(channel.channel_id, error_msg)
                        log(f"[{processed_count}/{remaining_channels}] ✗ {channel.name}: {error_msg}", "ERROR")
            
            # Salva checkpoint após cada batch
            checkpoint_manager.save_checkpoint()
            log(f"Checkpoint salvo após lote {batch_num + 1}")
            
            # Pequeno delay entre batches
            if batch_num < total_batches - 1:
                time.sleep(1)
        
        # Estatísticas finais
        elapsed_total = (datetime.now() - start_time).total_seconds()
        
        log(f"\n{'='*60}")
        log("ATUALIZAÇÃO CONCLUÍDA!", "SUCCESS")
        log(f"{'='*60}")
        log(f"Total de canais: {total_channels}")
        log(f"Processados com sucesso: {total_updated}", "SUCCESS")
        log(f"Erros: {total_errors}", "ERROR" if total_errors > 0 else "INFO")
        log(f"Já processados hoje: {len(processed_ids)}")
        log(f"Tempo total: {elapsed_total:.1f}s ({elapsed_total/60:.1f} minutos)")
        
        if remaining_channels > 0:
            log(f"Taxa de sucesso: {(total_updated/remaining_channels*100):.1f}%")
        
        # Exibe quota final
        quota_info = youtube_extractor.get_quota_info()
        log(f"\nQuota da API:")
        log(f"  Usada: {quota_info['used']}/{quota_info['limit']} ({quota_info['percentage_used']:.1f}%)")
        log(f"  Restante: {quota_info['remaining']} unidades")
        
        # Salva checkpoint final
        checkpoint_manager.save_checkpoint()
        
    except KeyboardInterrupt:
        log("\nProcessamento interrompido pelo usuário", "WARNING")
        checkpoint_manager.save_checkpoint()
        log("Checkpoint salvo. Execute novamente para continuar de onde parou.", "INFO")
        sys.exit(0)
    except Exception as e:
        log(f"Erro fatal: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        checkpoint_manager.save_checkpoint()
        sys.exit(1)


if __name__ == "__main__":
    # Carrega configurações de variáveis de ambiente (GitHub Secrets)
    if os.getenv("SUPABASE_URL"):
        config.SUPABASE_URL = os.getenv("SUPABASE_URL")
    if os.getenv("SUPABASE_KEY"):
        config.SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    if os.getenv("YOUTUBE_API_KEY"):
        config.YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
    if os.getenv("YOUTUBE_API_KEYS"):
        keys_str = os.getenv("YOUTUBE_API_KEYS")
        if keys_str:
            keys = [k.strip() for k in keys_str.split(',')]
            config.save_api_keys(keys)
    
    # Verifica se foi especificado um canal específico via variável de ambiente
    channel_id_env = os.getenv("CHANNEL_ID")
    channel_ids = None
    
    if channel_id_env:
        # Suporta um único channel_id ou múltiplos separados por vírgula
        channel_ids = [cid.strip() for cid in channel_id_env.split(',') if cid.strip()]
        log(f"Modo: Atualização de canal(is) específico(s): {len(channel_ids)} canal(is)", "INFO")
    else:
        log("Modo: Atualização de todos os canais", "INFO")
    
    update_all_channels(channel_ids=channel_ids)
