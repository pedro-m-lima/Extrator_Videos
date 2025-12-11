#!/usr/bin/env python3
"""
Script para verificar a estrutura da tabela historical_metrics no Supabase
"""

from supabase_client import SupabaseClient
from datetime import datetime

def check_historical_metrics():
    """Verifica se a tabela historical_metrics existe e sua estrutura"""
    client = SupabaseClient()
    
    print("="*80)
    print("VERIFICAÇÃO DA TABELA historical_metrics NO SUPABASE")
    print("="*80)
    print()
    
    # 1. Verifica tabela metrics (referência)
    print("1. VERIFICANDO TABELA 'metrics' (referência):")
    print("-" * 80)
    try:
        response = client.client.table('metrics').select('*').limit(1).execute()
        if response.data:
            print(f"   ✅ Tabela 'metrics' existe")
            print(f"   Estrutura das colunas: {list(response.data[0].keys())}")
            print(f"   Exemplo de registro:")
            for key, value in response.data[0].items():
                print(f"      - {key}: {value} (tipo: {type(value).__name__})")
        else:
            print("   ⚠️  Tabela 'metrics' existe mas está vazia")
    except Exception as e:
        print(f"   ❌ Erro ao acessar 'metrics': {e}")
    
    print()
    
    # 2. Verifica tabela historical_metrics
    print("2. VERIFICANDO TABELA 'historical_metrics':")
    print("-" * 80)
    try:
        # Tenta buscar um registro
        response = client.client.table('historical_metrics').select('*').limit(1).execute()
        
        if response.data:
            print(f"   ✅ Tabela 'historical_metrics' EXISTE")
            print(f"   Estrutura das colunas: {list(response.data[0].keys())}")
            print()
            print(f"   Exemplo de registro:")
            for key, value in response.data[0].items():
                print(f"      - {key}: {value} (tipo: {type(value).__name__})")
            print()
            
            # Conta total de registros
            count_response = client.client.table('historical_metrics').select('*', count='exact').limit(1).execute()
            print(f"   Total de registros na tabela: {count_response.count if hasattr(count_response, 'count') else 'N/A'}")
            
            # Busca alguns registros para análise
            print()
            print("   Amostra de registros (últimos 5):")
            sample = client.client.table('historical_metrics').select('*').order('created_at', desc=True).limit(5).execute()
            for i, record in enumerate(sample.data, 1):
                print(f"   Registro {i}:")
                for key, value in record.items():
                    print(f"      {key}: {value}")
                print()
        else:
            print("   ⚠️  Tabela 'historical_metrics' existe mas está VAZIA")
            
            # Tenta descobrir estrutura através de uma inserção de teste (que falhará mas mostrará estrutura)
            print("   Tentando descobrir estrutura da tabela...")
            try:
                # Tenta inserir um registro de teste (vai falhar mas pode mostrar estrutura esperada)
                client.client.table('historical_metrics').insert({
                    'test': 'test'
                }).execute()
            except Exception as e:
                error_msg = str(e)
                print(f"   Erro (esperado): {error_msg}")
                # O erro pode conter informações sobre a estrutura esperada
                
    except Exception as e:
        error_str = str(e).lower()
        if 'does not exist' in error_str or 'not found' in error_str or 'relation' in error_str:
            print(f"   ❌ Tabela 'historical_metrics' NÃO EXISTE no Supabase")
            print(f"   Erro: {e}")
        else:
            print(f"   ⚠️  Erro ao acessar 'historical_metrics': {e}")
            print(f"   (Pode ser que a tabela exista mas tenha problemas de permissão)")
    
    print()
    
    # 3. Verifica estrutura atual da tabela metrics para comparação
    print("3. ANÁLISE DA TABELA 'metrics' (para comparação):")
    print("-" * 80)
    try:
        # Busca alguns registros para ver padrão
        metrics_sample = client.client.table('metrics').select('*').order('date', desc=True).limit(10).execute()
        
        if metrics_sample.data:
            print(f"   Total de registros de exemplo: {len(metrics_sample.data)}")
            print()
            print("   Estrutura atual de 'metrics':")
            first_record = metrics_sample.data[0]
            for key in first_record.keys():
                print(f"      - {key}")
            print()
            print("   Exemplo de dados (últimos registros):")
            for record in metrics_sample.data[:3]:
                print(f"      Canal: {record.get('channel_id', 'N/A')[:20]}... | Data: {record.get('date')} | Views: {record.get('views', 0):,}")
    except Exception as e:
        print(f"   Erro ao analisar 'metrics': {e}")
    
    print()
    print("="*80)

if __name__ == "__main__":
    check_historical_metrics()

