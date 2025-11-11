"""
Script para atualizar estatísticas de todos os canais diariamente
"""
import os
import sys
from datetime import datetime
import config
from api_key_manager import APIKeyManager
from youtube_extractor import YouTubeExtractor
from supabase_client import SupabaseClient

def log(message, level="INFO"):
    """Log formatado"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} [{level}] {message}")

def update_all_channels():
    """Atualiza estatísticas de todos os canais"""
    try:
        # Inicializa clientes
        api_key_manager = APIKeyManager()
        youtube_extractor = YouTubeExtractor(api_key_manager)
        supabase_client = SupabaseClient()
        
        # Busca todos os canais
        log("Buscando canais...")
        channels = supabase_client.get_channels()
        
        if not channels:
            log("Nenhum canal encontrado", "WARNING")
            return
        
        log(f"Encontrados {len(channels)} canais para atualizar")
        
        total_updated = 0
        total_errors = 0
        
        for i, channel in enumerate(channels, 1):
            log(f"Processando canal {i}/{len(channels)}: {channel.name} (ID: {channel.channel_id})")
            
            try:
                # Busca estatísticas do canal
                stats = youtube_extractor.get_channel_statistics(channel.channel_id)
                
                if not stats:
                    log(f"  Erro: Não foi possível obter estatísticas do canal", "ERROR")
                    total_errors += 1
                    continue
                
                # Atualiza canal com histórico
                supabase_client.update_channel_with_history(
                    channel_id=channel.channel_id,
                    views=stats['views'],
                    subscribers=stats['subscribers'],
                    video_count=stats['video_count']
                )
                
                total_updated += 1
                log(f"  ✓ Atualizado: {stats['views']:,} views, {stats['subscribers']:,} inscritos, {stats['video_count']} vídeos")
                
                # Exibe quota
                quota_info = youtube_extractor.get_quota_info()
                if quota_info['remaining'] < 100:
                    log(f"  AVISO: Quota restante muito baixa ({quota_info['remaining']})", "WARNING")
                
            except Exception as e:
                log(f"  Erro ao processar canal: {e}", "ERROR")
                total_errors += 1
                continue
        
        log(f"\n{'='*50}")
        log(f"Atualização concluída!")
        log(f"Total processado: {len(channels)}")
        log(f"Atualizados com sucesso: {total_updated}")
        log(f"Erros: {total_errors}")
        
        # Exibe quota final
        quota_info = youtube_extractor.get_quota_info()
        log(f"Quota da API: {quota_info['used']}/{quota_info['limit']} usada ({quota_info['percentage_used']:.1f}%)")
        log(f"Quota restante: {quota_info['remaining']} unidades")
        
    except Exception as e:
        log(f"Erro fatal: {e}", "ERROR")
        import traceback
        traceback.print_exc()
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
    
    update_all_channels()

