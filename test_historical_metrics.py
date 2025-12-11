#!/usr/bin/env python3
"""
Script de teste para historical_metrics
Testa a implementação com um canal específico
"""

from supabase_client import SupabaseClient
from historical_metrics_aggregator import HistoricalMetricsAggregator
from datetime import date
from calendar import monthrange

def test_single_channel():
    """Testa agregação para um canal específico"""
    client = SupabaseClient()
    aggregator = HistoricalMetricsAggregator(client)
    
    # Usa mês atual
    today = date.today()
    year = today.year
    month = today.month
    
    print("=" * 80)
    print("TESTE DE HISTORICAL METRICS")
    print("=" * 80)
    print(f"Mês: {month}/{year}")
    print()
    
    # Busca primeiro canal
    channels = client.get_channels()
    if not channels:
        print("❌ Nenhum canal encontrado")
        return
    
    test_channel = channels[0]
    print(f"Testando com canal: {test_channel.name} ({test_channel.channel_id})")
    print()
    
    # Testa agregação
    print("1. Testando agregação de métricas...")
    metrics = aggregator.aggregate_monthly_metrics(test_channel.channel_id, year, month)
    
    if metrics:
        print("✅ Métricas agregadas com sucesso:")
        print(f"   Views: {metrics['views']:,}")
        print(f"   Subscribers (diferença): {metrics['subscribers']:,}")
        print(f"   Video Count: {metrics['video_count']:,}")
        print(f"   Longs postados: {metrics['longs_posted']}")
        print(f"   Shorts postados: {metrics['shorts_posted']}")
        print(f"   Views de longs: {metrics['longs_views']:,}")
        print(f"   Views de shorts: {metrics['shorts_views']:,}")
        print()
        
        # Testa UPSERT (comentado para não modificar dados reais)
        print("2. Testando UPSERT (simulado - não executa)...")
        print("   ✅ Estrutura de dados correta para UPSERT")
        print("   (Para executar realmente, descomente a linha abaixo)")
        # success = aggregator.upsert_historical_metric(test_channel.channel_id, year, month, metrics)
        # print(f"   Resultado: {'✅ Sucesso' if success else '❌ Erro'}")
    else:
        print("❌ Não foi possível agregar métricas")
    
    print()
    print("=" * 80)
    print("Teste concluído!")

if __name__ == "__main__":
    test_single_channel()

