#!/usr/bin/env python3
"""
Script para atualizar historical_metrics
Pode ser executado diariamente pela cron job
"""

import os
import sys
from datetime import datetime, date
from calendar import monthrange
from supabase_client import SupabaseClient
from historical_metrics_aggregator import HistoricalMetricsAggregator
import config

def log(message: str, level: str = "INFO"):
    """Log simples"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = {
        "INFO": "ℹ️",
        "WARNING": "⚠️",
        "ERROR": "❌",
        "SUCCESS": "✅"
    }.get(level, "ℹ️")
    print(f"[{timestamp}] {prefix} {message}")
    sys.stdout.flush()

def main():
    """Função principal"""
    try:
        # Carrega configurações de variáveis de ambiente (GitHub Secrets)
        if os.getenv('SUPABASE_URL'):
            config.SUPABASE_URL = os.getenv('SUPABASE_URL')
        if os.getenv('SUPABASE_KEY'):
            config.SUPABASE_KEY = os.getenv('SUPABASE_KEY')
        
        log("=" * 80)
        log("ATUALIZAÇÃO DE HISTORICAL METRICS")
        log("=" * 80)
        log(f"Iniciando em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log("")
        
        # Inicializa cliente do Supabase
        supabase_client = SupabaseClient()
        aggregator = HistoricalMetricsAggregator(supabase_client)
        
        # Processa mês atual
        log("Processando mês atual...")
        stats = aggregator.process_current_month()
        log("")
        log(f"✅ Processamento concluído:")
        log(f"   - Canais processados: {stats['channels_processed']}")
        log(f"   - Canais atualizados: {stats['channels_updated']}")
        log(f"   - Canais criados: {stats['channels_created']}")
        log(f"   - Canais pulados: {stats['channels_skipped']}")
        log(f"   - Erros: {stats['errors']}")
        log("")
        
        # Verifica se é último dia do mês
        today = date.today()
        last_day = monthrange(today.year, today.month)[1]
        
        if today.day == last_day:
            log(f"⚠️  Último dia do mês detectado ({today.day}/{last_day})")
            log("Criando entradas para o próximo mês...")
            log("")
            
            next_month_stats = aggregator.create_next_month_entries()
            log("")
            log(f"✅ Criação de entradas concluída:")
            log(f"   - Entradas criadas: {next_month_stats['entries_created']}")
            log(f"   - Entradas já existentes: {next_month_stats['entries_skipped']}")
            log(f"   - Erros: {next_month_stats['errors']}")
        else:
            log(f"ℹ️  Não é o último dia do mês ({today.day}/{last_day}), pulando criação de entradas do próximo mês")
        
        log("")
        log("=" * 80)
        log("✅ Execução concluída com sucesso!")
        log("=" * 80)
        return True
        
    except Exception as e:
        log(f"❌ Erro fatal: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

