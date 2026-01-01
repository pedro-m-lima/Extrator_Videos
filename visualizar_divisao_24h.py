#!/usr/bin/env python3
"""
Script para visualizar a divisão de 24 horas por segmento
Mostra como ficaria a distribuição antes de implementar
"""
from datetime import datetime, timedelta

def print_divisao_24h():
    """Imprime a divisão de 24 horas por segmento (a cada 2 horas = 12 slots)"""
    
    print("=" * 80)
    print("DIVISÃO DE 24 HORAS POR SEGMENTO - PROPOSTA (A CADA 2 HORAS)")
    print("=" * 80)
    print()
    
    # Dias pares = Fitness, Dias ímpares = Podcast
    print("📅 REGRA DE SEGMENTO:")
    print("   • Dias PARES (2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30) → FITNESS")
    print("   • Dias ÍMPARES (1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31) → PODCAST")
    print()
    
    print("=" * 80)
    print("DIAS PARES - FITNESS (12 slots, a cada 2 horas)")
    print("=" * 80)
    print(f"{'Slot':<6} {'Hora BRT':<12} {'Hora UTC':<12} {'Descrição':<20}")
    print("-" * 80)
    
    total_slots = 12
    for slot in range(total_slots):
        hora_brt = slot * 2  # A cada 2 horas: 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22
        hora_brt_str = f"{hora_brt:02d}:00"
        hora_utc = (hora_brt + 3) % 24  # BRT = UTC-3
        hora_utc_str = f"{hora_utc:02d}:00"
        
        if hora_brt == 0:
            desc = "Meia-noite"
        elif 2 <= hora_brt <= 6:
            desc = "Madrugada"
        elif 8 <= hora_brt <= 12:
            desc = "Manhã"
        elif 14 <= hora_brt <= 18:
            desc = "Tarde"
        else:
            desc = "Noite"
        
        print(f"{slot:<6} {hora_brt_str:<12} {hora_utc_str:<12} {desc:<20}")
    
    print()
    print("=" * 80)
    print("DIAS ÍMPARES - PODCAST (12 slots, a cada 2 horas)")
    print("=" * 80)
    print(f"{'Slot':<6} {'Hora BRT':<12} {'Hora UTC':<12} {'Descrição':<20}")
    print("-" * 80)
    
    for slot in range(total_slots):
        hora_brt = slot * 2  # A cada 2 horas: 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22
        hora_brt_str = f"{hora_brt:02d}:00"
        hora_utc = (hora_brt + 3) % 24  # BRT = UTC-3
        hora_utc_str = f"{hora_utc:02d}:00"
        
        if hora_brt == 0:
            desc = "Meia-noite"
        elif 2 <= hora_brt <= 6:
            desc = "Madrugada"
        elif 8 <= hora_brt <= 12:
            desc = "Manhã"
        elif 14 <= hora_brt <= 18:
            desc = "Tarde"
        else:
            desc = "Noite"
        
        print(f"{slot:<6} {hora_brt_str:<12} {hora_utc_str:<12} {desc:<20}")
    
    print()
    print("=" * 80)
    print("EXEMPLO DE DISTRIBUIÇÃO DE CANAIS")
    print("=" * 80)
    print()
    
    # Exemplo com números
    fitness_canais = 100
    podcast_canais = 50
    total_slots = 12  # A cada 2 horas = 12 slots
    
    print(f"📊 Exemplo com {fitness_canais} canais Fitness e {podcast_canais} canais Podcast:")
    print()
    
    print("DIAS PARES (Fitness):")
    canais_por_slot_fitness = fitness_canais / total_slots
    print(f"   • Total de canais: {fitness_canais}")
    print(f"   • Canais por slot: ~{canais_por_slot_fitness:.2f} (distribuídos em {total_slots} slots)")
    print(f"   • Exemplo Slot 0 (00:00 BRT): ~{int(canais_por_slot_fitness)} canais")
    print(f"   • Exemplo Slot 6 (12:00 BRT): ~{int(canais_por_slot_fitness)} canais")
    print(f"   • Exemplo Slot 11 (22:00 BRT): ~{int(canais_por_slot_fitness)} canais")
    print()
    
    print("DIAS ÍMPARES (Podcast):")
    canais_por_slot_podcast = podcast_canais / total_slots
    print(f"   • Total de canais: {podcast_canais}")
    print(f"   • Canais por slot: ~{canais_por_slot_podcast:.2f} (distribuídos em {total_slots} slots)")
    print(f"   • Exemplo Slot 0 (00:00 BRT): ~{int(canais_por_slot_podcast)} canais")
    print(f"   • Exemplo Slot 6 (12:00 BRT): ~{int(canais_por_slot_podcast)} canais")
    print(f"   • Exemplo Slot 11 (22:00 BRT): ~{int(canais_por_slot_podcast)} canais")
    print()
    
    print("=" * 80)
    print("CALENDÁRIO DE EXECUÇÃO - EXEMPLO (Janeiro 2025)")
    print("=" * 80)
    print()
    
    # Mostra alguns dias de exemplo
    print(f"{'Data':<12} {'Dia':<6} {'Segmento':<10} {'Execuções':<12} {'Horários'}")
    print("-" * 80)
    
    for dia in range(1, 8):
        is_par = (dia % 2) == 0
        segmento = "Fitness" if is_par else "Podcast"
        tipo_dia = "Par" if is_par else "Ímpar"
        print(f"01/{dia:02d}/2025  {tipo_dia:<6} {segmento:<10} 12 slots    00:00, 02:00, 04:00... 22:00 BRT")
    
    print()
    print("=" * 80)
    print("COMPARAÇÃO: ANTES vs. DEPOIS")
    print("=" * 80)
    print()
    
    print("❌ ANTES (Atual):")
    print("   • 5 slots por dia (1h, 3h, 5h, 7h, 9h BRT)")
    print("   • Dias pares: Fitness (5 slots)")
    print("   • Dias ímpares: Podcast (5 slots)")
    print("   • Total: 10 execuções por dia")
    print("   • Canais Fitness por slot: ~20 canais (100 ÷ 5)")
    print("   • Canais Podcast por slot: ~10 canais (50 ÷ 5)")
    print()
    
    print("✅ DEPOIS (Proposta - A cada 2 horas):")
    print("   • 12 slots por dia (a cada 2 horas: 0h, 2h, 4h, 6h, 8h, 10h, 12h, 14h, 16h, 18h, 20h, 22h BRT)")
    print("   • Dias pares: Fitness (12 slots)")
    print("   • Dias ímpares: Podcast (12 slots)")
    print("   • Total: 12 execuções por dia (alternadas)")
    print("   • Canais Fitness por slot: ~8 canais (100 ÷ 12)")
    print("   • Canais Podcast por slot: ~4 canais (50 ÷ 12)")
    print()
    
    print("=" * 80)
    print("VANTAGENS DA NOVA DIVISÃO")
    print("=" * 80)
    print()
    print("✅ Cobertura completa: 24 horas de processamento por segmento (a cada 2h)")
    print("✅ Distribuição uniforme: Carga distribuída ao longo do dia")
    print("✅ Maior frequência: Atualizações mais frequentes dos vídeos (12x por dia)")
    print("✅ Menor carga por execução: Menos canais por slot = execuções mais rápidas")
    print("✅ Melhor uso de recursos: Processamento distribuído ao longo do dia")
    print("✅ Equilíbrio: Mais slots que antes (5→12) mas não excessivo (24)")
    print()
    
    print("=" * 80)
    print("CONSIDERAÇÕES IMPORTANTES")
    print("=" * 80)
    print()
    print("⚠️  12 execuções por dia: 12 fitness + 12 podcast (alternados)")
    print("⚠️  Quota da API: Verificar se há quota suficiente para 12 execuções diárias")
    print("⚠️  Custo: Mais execuções que antes (10→12) mas ainda gerenciável")
    print("⚠️  Cron: GitHub Actions usará '0 */2 * * *' (a cada 2 horas) e determinará segmento/slot no código")
    print()
    
    print("=" * 80)
    print("CONFIGURAÇÃO GITHUB ACTIONS (Cron)")
    print("=" * 80)
    print()
    print("Como o GitHub Actions não suporta especificar dias específicos do mês")
    print("no cron, usaremos uma única regra que executa a cada 2 horas:")
    print()
    print("  schedule:")
    print("    - cron: '0 */2 * * *'  # Executa a cada 2 horas (0h, 2h, 4h, 6h, 8h, 10h, 12h, 14h, 16h, 18h, 20h, 22h)")
    print()
    print("O código Python determinará automaticamente:")
    print("  • Segmento baseado no dia (par = Fitness, ímpar = Podcast)")
    print("  • Slot baseado na hora atual (0-11, onde 0=00h, 1=02h, 2=04h, ..., 11=22h)")
    print()
    
    print("=" * 80)

if __name__ == "__main__":
    print_divisao_24h()

