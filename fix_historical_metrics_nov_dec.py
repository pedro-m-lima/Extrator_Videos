#!/usr/bin/env python3
"""
Script para corrigir dados de historical_metrics de novembro e dezembro de 2025
Recalcula shorts/longs usando a nova lógica corrigida
"""

import sys
from datetime import datetime
from mysql_client import MySQLClient
from historical_metrics_aggregator import HistoricalMetricsAggregator

def format_number(num):
    """Formata número com separador de milhares"""
    return f"{num:,}" if num else "0"

def show_comparison(before, after, channel_name):
    """Mostra comparação entre dados antes e depois"""
    print(f"\n{'='*100}")
    print(f"Canal: {channel_name}")
    print(f"  Channel ID: {before['channel_id']}")
    print(f"  Período: {before['month']:02d}/{before['year']}")
    print(f"{'-'*100}")
    print(f"{'Métrica':<30} | {'ANTES':<20} | {'DEPOIS':<20} | {'DIFERENÇA':<15}")
    print(f"{'-'*100}")
    
    metrics_to_compare = [
        ('longs_posted', 'Longs Postados'),
        ('shorts_posted', 'Shorts Postados'),
        ('longs_views', 'Views de Longs'),
        ('shorts_views', 'Views de Shorts'),
    ]
    
    for key, label in metrics_to_compare:
        before_val = before.get(key, 0) or 0
        after_val = after.get(key, 0) or 0
        diff = after_val - before_val
        diff_str = f"+{diff:,}" if diff > 0 else f"{diff:,}" if diff < 0 else "0"
        
        if key.endswith('_views'):
            before_str = format_number(before_val)
            after_str = format_number(after_val)
        else:
            before_str = str(before_val)
            after_str = str(after_val)
        
        print(f"{label:<30} | {before_str:<20} | {after_str:<20} | {diff_str:<15}")
    
    print(f"{'='*100}\n")

def main():
    """Função principal"""
    try:
        print("="*100)
        print("CORREÇÃO DE HISTORICAL_METRICS - NOVEMBRO E DEZEMBRO 2025")
        print("="*100)
        print(f"Iniciando em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Inicializa clientes
        client = MySQLClient()
        aggregator = HistoricalMetricsAggregator(client)
        
        # Meses a processar
        months_to_process = [
            (2025, 11, "Novembro"),
            (2025, 12, "Dezembro")
        ]
        
        # Busca todos os canais
        channels = client.get_channels()
        print(f"Encontrados {len(channels)} canais para processar")
        print()
        
        # Dicionário para armazenar comparações
        all_comparisons = []
        
        # Processa cada mês
        for year, month, month_name in months_to_process:
            print(f"\n{'#'*100}")
            print(f"PROCESSANDO {month_name.upper()} DE {year}")
            print(f"{'#'*100}\n")
            
            for i, channel in enumerate(channels, 1):
                channel_id = channel.channel_id
                channel_name = channel.name
                
                print(f"[{i}/{len(channels)}] Processando: {channel_name} ({channel_id[:20]}...)")
                
                try:
                    # Busca dados atuais do banco
                    connection = client._get_connection()
                    cursor = connection.cursor(dictionary=True)
                    cursor.execute('''
                        SELECT * FROM historical_metrics 
                        WHERE channel_id = %s AND year = %s AND month = %s
                    ''', (channel_id, year, month))
                    before_data = cursor.fetchone()
                    cursor.close()
                    connection.close()
                    
                    if not before_data:
                        print(f"  ⚠️  Nenhum registro encontrado para este mês, pulando...")
                        continue
                    
                    # Recalcula usando a nova lógica
                    new_metrics = aggregator.aggregate_monthly_metrics(channel_id, year, month)
                    
                    if not new_metrics:
                        print(f"  ⚠️  Não foi possível recalcular métricas, pulando...")
                        continue
                    
                    # Prepara dados para comparação
                    before_dict = {
                        'channel_id': before_data['channel_id'],
                        'year': before_data['year'],
                        'month': before_data['month'],
                        'longs_posted': before_data.get('longs_posted', 0) or 0,
                        'shorts_posted': before_data.get('shorts_posted', 0) or 0,
                        'longs_views': before_data.get('longs_views', 0) or 0,
                        'shorts_views': before_data.get('shorts_views', 0) or 0,
                    }
                    
                    after_dict = {
                        'channel_id': new_metrics['channel_id'],
                        'year': new_metrics['year'],
                        'month': new_metrics['month'],
                        'longs_posted': new_metrics.get('longs_posted', 0) or 0,
                        'shorts_posted': new_metrics.get('shorts_posted', 0) or 0,
                        'longs_views': new_metrics.get('longs_views', 0) or 0,
                        'shorts_views': new_metrics.get('shorts_views', 0) or 0,
                    }
                    
                    # Verifica se há diferença
                    has_changes = (
                        before_dict['longs_posted'] != after_dict['longs_posted'] or
                        before_dict['shorts_posted'] != after_dict['shorts_posted'] or
                        before_dict['longs_views'] != after_dict['longs_views'] or
                        before_dict['shorts_views'] != after_dict['shorts_views']
                    )
                    
                    if has_changes:
                        all_comparisons.append({
                            'channel_name': channel_name,
                            'channel_id': channel_id,
                            'year': year,
                            'month': month,
                            'month_name': month_name,
                            'before': before_dict,
                            'after': after_dict,
                            'new_metrics': new_metrics
                        })
                        print(f"  ✅ Dados recalculados (há diferenças)")
                    else:
                        print(f"  ℹ️  Dados já estão corretos")
                        
                except Exception as e:
                    print(f"  ❌ Erro ao processar: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
        
        # Mostra resumo de comparações
        print(f"\n{'='*100}")
        print("RESUMO DAS ALTERAÇÕES")
        print(f"{'='*100}\n")
        
        if not all_comparisons:
            print("✅ Nenhuma alteração necessária! Todos os dados já estão corretos.")
            return True
        
        print(f"Total de registros que serão atualizados: {len(all_comparisons)}\n")
        
        # Mostra todas as comparações
        for comp in all_comparisons:
            show_comparison(
                comp['before'],
                comp['after'],
                f"{comp['channel_name']} - {comp['month_name']}/{comp['year']}"
            )
        
        # Salva relatório em arquivo
        report_file = "historical_metrics_correction_report.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("="*100 + "\n")
            f.write("RELATÓRIO DE CORREÇÃO - HISTORICAL_METRICS\n")
            f.write("Novembro e Dezembro de 2025\n")
            f.write("="*100 + "\n\n")
            f.write(f"Total de registros que serão atualizados: {len(all_comparisons)}\n\n")
            
            for comp in all_comparisons:
                f.write("="*100 + "\n")
                f.write(f"Canal: {comp['channel_name']} - {comp['month_name']}/{comp['year']}\n")
                f.write(f"  Channel ID: {comp['before']['channel_id']}\n")
                f.write("-"*100 + "\n")
                f.write(f"{'Métrica':<30} | {'ANTES':<20} | {'DEPOIS':<20} | {'DIFERENÇA':<15}\n")
                f.write("-"*100 + "\n")
                
                metrics_to_compare = [
                    ('longs_posted', 'Longs Postados'),
                    ('shorts_posted', 'Shorts Postados'),
                    ('longs_views', 'Views de Longs'),
                    ('shorts_views', 'Views de Shorts'),
                ]
                
                for key, label in metrics_to_compare:
                    before_val = comp['before'].get(key, 0) or 0
                    after_val = comp['after'].get(key, 0) or 0
                    diff = after_val - before_val
                    diff_str = f"+{diff:,}" if diff > 0 else f"{diff:,}" if diff < 0 else "0"
                    
                    if key.endswith('_views'):
                        before_str = format_number(before_val)
                        after_str = format_number(after_val)
                    else:
                        before_str = str(before_val)
                        after_str = str(after_val)
                    
                    f.write(f"{label:<30} | {before_str:<20} | {after_str:<20} | {diff_str:<15}\n")
                
                f.write("="*100 + "\n\n")
        
        print(f"\n{'='*100}")
        print("RELATÓRIO SALVO")
        print(f"{'='*100}")
        print(f"\n✅ Relatório completo salvo em: {report_file}")
        print(f"\nTotal de {len(all_comparisons)} registros serão atualizados.")
        print(f"\nPara aplicar as correções, execute:")
        print(f"  python fix_historical_metrics_nov_dec.py --apply")
        print(f"\n{'='*100}\n")
        
        # Verifica se foi passado --apply como argumento
        if '--apply' in sys.argv:
            print("Modo --apply detectado. Aplicando alterações...\n")
        else:
            print("⚠️  Modo de visualização. Use --apply para aplicar as alterações.")
            return True
        
        # Verifica se deve aplicar (só se --apply foi passado)
        if '--apply' not in sys.argv:
            return True
        
        # Aplica as alterações
        print(f"\n{'='*100}")
        print("APLICANDO ALTERAÇÕES NO BANCO DE DADOS")
        print(f"{'='*100}\n")
        
        updated = 0
        errors = 0
        
        for i, comp in enumerate(all_comparisons, 1):
            try:
                print(f"[{i}/{len(all_comparisons)}] Atualizando {comp['channel_name']} - {comp['month_name']}/{comp['year']}...")
                
                success = aggregator.upsert_historical_metric(
                    comp['channel_id'],
                    comp['year'],
                    comp['month'],
                    comp['new_metrics']
                )
                
                if success:
                    updated += 1
                    print(f"  ✅ Atualizado com sucesso")
                else:
                    errors += 1
                    print(f"  ❌ Erro ao atualizar")
                    
            except Exception as e:
                errors += 1
                print(f"  ❌ Erro: {e}")
                import traceback
                traceback.print_exc()
        
        # Resumo final
        print(f"\n{'='*100}")
        print("RESUMO FINAL")
        print(f"{'='*100}")
        print(f"✅ Registros atualizados: {updated}")
        if errors > 0:
            print(f"❌ Erros: {errors}")
        print(f"{'='*100}\n")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Erro fatal: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

