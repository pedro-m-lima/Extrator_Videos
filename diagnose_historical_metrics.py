#!/usr/bin/env python3
"""
Script de diagnóstico para verificar por que historical_metrics não está atualizando
"""
import os
import sys
from datetime import date, datetime
from calendar import monthrange
import config
from supabase_client import SupabaseClient
from historical_metrics_aggregator import HistoricalMetricsAggregator

def log(message: str, level: str = "INFO"):
    """Adiciona mensagem aos logs"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = {
        "INFO": "[INFO]",
        "SUCCESS": "[✓]",
        "ERROR": "[✗]",
        "WARNING": "[!]"
    }.get(level, "[INFO]")
    
    print(f"{timestamp} {prefix} {message}")
    sys.stdout.flush()

def main():
    try:
        # Carrega configurações
        if os.getenv('MYSQL_HOST'):
            config.MYSQL_HOST = os.getenv('MYSQL_HOST')
        if os.getenv('MYSQL_PORT'):
            config.MYSQL_PORT = int(os.getenv('MYSQL_PORT', "3306"))
        if os.getenv('MYSQL_USER'):
            config.MYSQL_USER = os.getenv('MYSQL_USER')
        if os.getenv('MYSQL_PASSWORD'):
            config.MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
        if os.getenv('MYSQL_DATABASE'):
            config.MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
        
        log("=" * 80)
        log("DIAGNÓSTICO: HISTORICAL METRICS", "INFO")
        log("=" * 80)
        log("")
        
        client = SupabaseClient()
        aggregator = HistoricalMetricsAggregator(client)
        
        today = date.today()
        year = today.year
        month = today.month
        
        log(f"Data atual: {today}")
        log(f"Mês sendo processado: {month}/{year}")
        log("")
        
        # 1. Verifica se há canais
        log("1. Verificando canais...")
        channels = client.get_channels()
        log(f"   Total de canais: {len(channels)}", "SUCCESS" if channels else "ERROR")
        log("")
        
        # 2. Verifica se há métricas na tabela metrics para o mês atual
        log("2. Verificando tabela 'metrics' para o mês atual...")
        first_day = date(year, month, 1)
        last_day = date(year, month, monthrange(year, month)[1])
        
        connection = client._get_connection()
        cursor = connection.cursor(dictionary=True)
        
        try:
            query = """
                SELECT COUNT(DISTINCT channel_id) as channels_with_metrics,
                       COUNT(*) as total_metrics,
                       MIN(date) as first_date,
                       MAX(date) as last_date
                FROM metrics 
                WHERE date >= %s AND date <= %s
            """
            cursor.execute(query, (first_day.isoformat(), last_day.isoformat()))
            result = cursor.fetchone()
            
            if result:
                log(f"   Canais com métricas no mês: {result['channels_with_metrics']}", 
                    "SUCCESS" if result['channels_with_metrics'] > 0 else "WARNING")
                log(f"   Total de registros de métricas: {result['total_metrics']}")
                log(f"   Primeira data: {result['first_date']}")
                log(f"   Última data: {result['last_date']}")
            else:
                log("   Nenhuma métrica encontrada para o mês atual!", "ERROR")
        finally:
            cursor.close()
            if connection and connection.is_connected():
                connection.close()
        log("")
        
        # 3. Verifica historical_metrics do mês atual
        log("3. Verificando 'historical_metrics' do mês atual...")
        connection = client._get_connection()
        cursor = connection.cursor(dictionary=True)
        
        try:
            query = """
                SELECT COUNT(*) as total,
                       COUNT(DISTINCT channel_id) as channels,
                       MAX(updated_at) as last_update
                FROM historical_metrics 
                WHERE year = %s AND month = %s
            """
            cursor.execute(query, (year, month))
            result = cursor.fetchone()
            
            if result:
                log(f"   Registros existentes: {result['total']}")
                log(f"   Canais com histórico: {result['channels']}")
                log(f"   Última atualização: {result['last_update']}")
            else:
                log("   Nenhum registro encontrado para o mês atual!", "WARNING")
        finally:
            cursor.close()
            if connection and connection.is_connected():
                connection.close()
        log("")
        
        # 4. Testa processamento de um canal
        log("4. Testando processamento de um canal...")
        if channels:
            test_channel = channels[0]
            log(f"   Testando canal: {test_channel.name} ({test_channel.channel_id})")
            
            try:
                metrics = aggregator.aggregate_monthly_metrics(test_channel.channel_id, year, month)
                if metrics:
                    log(f"   ✓ Métricas agregadas com sucesso:", "SUCCESS")
                    log(f"      - Views: {metrics.get('views', 0):,}")
                    log(f"      - Subscribers: {metrics.get('subscribers', 0):,}")
                    log(f"      - Video count: {metrics.get('video_count', 0):,}")
                    log(f"      - Longs: {metrics.get('longs_posted', 0)}")
                    log(f"      - Shorts: {metrics.get('shorts_posted', 0)}")
                else:
                    log(f"   ✗ Não foi possível agregar métricas para este canal", "ERROR")
                    log(f"      Possíveis causas:", "WARNING")
                    log(f"      - Sem dados na tabela 'metrics' para este mês", "WARNING")
                    log(f"      - Sem vídeos publicados neste mês", "WARNING")
            except Exception as e:
                log(f"   ✗ Erro ao processar: {e}", "ERROR")
                import traceback
                traceback.print_exc()
        log("")
        
        # 5. Recomendações
        log("=" * 80)
        log("RECOMENDAÇÕES:", "INFO")
        log("=" * 80)
        log("")
        log("Se historical_metrics não está atualizando:", "INFO")
        log("  1. Verifique se o workflow 'Atualizar Estatísticas dos Canais' está rodando", "INFO")
        log("  2. Verifique se a tabela 'metrics' tem dados do mês atual", "INFO")
        log("  3. Execute manualmente: python update_historical_metrics.py", "INFO")
        log("  4. Verifique os logs do workflow 'Extrair Vídeos do YouTube'", "INFO")
        log("")
        
    except Exception as e:
        log(f"Erro no diagnóstico: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()


