#!/usr/bin/env python3
"""
Script para gerar exemplo de dados agregados para historical_metrics
Mostra como ficariam os dados para os últimos 10 canais usando dados reais do Supabase
"""

from supabase_client import SupabaseClient
from models import Channel, Video
from utils import parse_iso8601_duration
from datetime import datetime, date
from calendar import monthrange

def parse_iso8601_duration(duration_str):
    """Converte duração ISO 8601 para segundos"""
    if not duration_str:
        return 0
    
    try:
        # Formato: PT1H2M3S ou PT2M30S ou PT30S
        duration_str = duration_str.upper().replace('PT', '')
        hours = 0
        minutes = 0
        seconds = 0
        
        if 'H' in duration_str:
            parts = duration_str.split('H')
            hours = int(parts[0])
            duration_str = parts[1] if len(parts) > 1 else ''
        
        if 'M' in duration_str:
            parts = duration_str.split('M')
            minutes = int(parts[0])
            duration_str = parts[1] if len(parts) > 1 else ''
        
        if 'S' in duration_str:
            seconds = int(duration_str.replace('S', ''))
        
        return hours * 3600 + minutes * 60 + seconds
    except:
        return 0

def get_example_month_data():
    """Gera exemplo de dados agregados para o mês atual usando dados reais"""
    client = SupabaseClient()
    
    # Usa o mês atual
    today = date.today()
    example_year = today.year
    example_month = today.month
    
    print("="*100)
    print("EXEMPLO DE DADOS AGREGADOS PARA historical_metrics")
    print("="*100)
    print(f"\nMês de exemplo: {example_month}/{example_year} (MÊS ATUAL)")
    print("="*100)
    print()
    
    # Busca todos os canais
    all_channels = client.get_channels()
    
    # Pega os últimos 10 canais (mais recentes)
    channels_to_process = all_channels[-10:] if len(all_channels) >= 10 else all_channels
    
    print(f"Processando {len(channels_to_process)} canais (últimos 10 canais)...\n")
    
    # Calcula range de datas do mês
    first_day = date(example_year, example_month, 1)
    last_day = date(example_year, example_month, monthrange(example_year, example_month)[1])
    
    print(f"Período: {first_day.isoformat()} até {last_day.isoformat()}")
    print(f"Data atual: {today.isoformat()} (dia {today.day} do mês)\n")
    print("="*100)
    print()
    
    results = []
    
    for i, channel in enumerate(channels_to_process, 1):
        print(f"\n{'='*100}")
        print(f"CANAL {i}/{len(channels_to_process)}: {channel.name}")
        print(f"{'='*100}")
        print(f"Channel ID: {channel.channel_id}")
        print()
        
        # 1. Busca dados de metrics (primeiro e último dia do mês para calcular subscribers)
        print("1. DADOS DE 'metrics' (métricas diárias do mês):")
        print("-" * 100)
        try:
            # Busca todas as métricas do mês
            metrics_response = client.client.table('metrics').select('*').eq('channel_id', channel.channel_id).gte('date', first_day.isoformat()).lte('date', last_day.isoformat()).order('date', desc=False).execute()
            
            if metrics_response.data and len(metrics_response.data) > 0:
                # Primeira métrica do mês (para calcular subscribers)
                first_metric = metrics_response.data[0]
                first_date = first_metric.get('date')
                first_subscribers = first_metric.get('subscribers', 0)
                
                # Última métrica do mês (para views, subscribers final, video_count)
                last_metric = metrics_response.data[-1]
                last_date = last_metric.get('date')
                views_from_metrics = last_metric.get('views', 0)
                subscribers_final = last_metric.get('subscribers', 0)
                video_count_from_metrics = last_metric.get('video_count', 0)
                
                # Calcula diferença de subscribers
                subscribers_diff = subscribers_final - first_subscribers
                
                print(f"   ✅ Encontrados {len(metrics_response.data)} registros no mês")
                print(f"      Primeira métrica: {first_date} (subscribers: {first_subscribers:,})")
                print(f"      Última métrica: {last_date} (subscribers: {subscribers_final:,})")
                print(f"      Diferença de subscribers: {subscribers_diff:,}")
                print(f"      Views (último dia): {views_from_metrics:,}")
                print(f"      Video Count (último dia): {video_count_from_metrics:,}")
            else:
                # Se não tem métricas no mês, busca a mais recente antes do mês
                print(f"   ⚠️  Nenhum registro encontrado no mês, buscando último registro anterior...")
                metrics_before = client.client.table('metrics').select('*').eq('channel_id', channel.channel_id).lt('date', first_day.isoformat()).order('date', desc=True).limit(1).execute()
                
                if metrics_before.data:
                    metric = metrics_before.data[0]
                    views_from_metrics = metric.get('views', 0)
                    subscribers_final = metric.get('subscribers', 0)
                    video_count_from_metrics = metric.get('video_count', 0)
                    metric_date = metric.get('date')
                    print(f"   ✅ Último registro anterior: {metric_date}")
                    print(f"      Views: {views_from_metrics:,}")
                    print(f"      Subscribers: {subscribers_final:,}")
                    print(f"      Video Count: {video_count_from_metrics:,}")
                    subscribers_diff = 0  # Sem dados do início do mês
                else:
                    print(f"   ❌ Nenhum registro encontrado")
                    views_from_metrics = 0
                    subscribers_final = 0
                    video_count_from_metrics = 0
                    subscribers_diff = 0
        except Exception as e:
            print(f"   ❌ Erro ao buscar metrics: {e}")
            views_from_metrics = 0
            subscribers_final = 0
            video_count_from_metrics = 0
            subscribers_diff = 0
        
        print()
        
        # 2. Busca vídeos publicados no mês
        print("2. DADOS DE 'videos' (publicados no mês):")
        print("-" * 100)
        try:
            # Busca todos os vídeos do canal
            all_videos = client.get_videos_by_channel(channel.channel_id)
            
            # Filtra vídeos publicados no mês
            videos_in_month = []
            for video in all_videos:
                if video.published_at:
                    try:
                        pub_date_str = video.published_at
                        # Remove 'Z' e converte
                        if pub_date_str.endswith('Z'):
                            pub_date_str = pub_date_str[:-1] + '+00:00'
                        pub_date = datetime.fromisoformat(pub_date_str)
                        if pub_date.year == example_year and pub_date.month == example_month:
                            videos_in_month.append(video)
                    except Exception as e:
                        continue
            
            print(f"   Total de vídeos do canal: {len(all_videos):,}")
            print(f"   Vídeos publicados em {example_month}/{example_year}: {len(videos_in_month):,}")
            print()
            
            # Calcula agregados
            longs_posted = 0
            shorts_posted = 0
            longs_views = 0
            shorts_views = 0
            
            for video in videos_in_month:
                views = video.views or 0
                
                # Verifica se é short ou longo
                if video.is_invalid:
                    continue  # Ignora vídeos inválidos
                
                # Verifica primeiro pelo campo is_short
                is_short_flag = video.is_short if hasattr(video, 'is_short') and video.is_short is not None else None
                
                # Se não tem is_short, verifica pela duração
                if is_short_flag is None:
                    if not video.duration:
                        continue  # Sem duração, não conta
                    
                    duration_seconds = parse_iso8601_duration(video.duration)
                    
                    if duration_seconds > 180:  # > 3 minutos = vídeo longo
                        longs_posted += 1
                        longs_views += views
                    elif duration_seconds > 0:  # > 0 e <= 3 minutos = short
                        shorts_posted += 1
                        shorts_views += views
                else:
                    # Usa o campo is_short
                    if is_short_flag:
                        shorts_posted += 1
                        shorts_views += views
                    else:
                        longs_posted += 1
                        longs_views += views
            
            print(f"   📊 Agregados calculados:")
            print(f"      Longs postados: {longs_posted:,}")
            print(f"      Shorts postados: {shorts_posted:,}")
            print(f"      Views de longs: {longs_views:,}")
            print(f"      Views de shorts: {shorts_views:,}")
            
        except Exception as e:
            print(f"   ❌ Erro ao buscar vídeos: {e}")
            import traceback
            traceback.print_exc()
            longs_posted = 0
            shorts_posted = 0
            longs_views = 0
            shorts_views = 0
        
        print()
        
        # 3. Mostra como ficaria o registro em historical_metrics
        print("3. REGISTRO QUE SERIA INSERIDO/ATUALIZADO EM 'historical_metrics':")
        print("-" * 100)
        print(f"   {{")
        print(f"     'channel_id': '{channel.channel_id}',")
        print(f"     'year': {example_year},")
        print(f"     'month': {example_month},")
        print(f"     'views': {views_from_metrics:,},  # ← do último dia de metrics")
        print(f"     'subscribers': {subscribers_diff:,},  # ← diferença (final - inicial) do mês")
        print(f"     'video_count': {video_count_from_metrics:,},  # ← do último dia de metrics")
        print(f"     'longs_posted': {longs_posted:,},  # ← contagem de vídeos longos publicados no mês")
        print(f"     'shorts_posted': {shorts_posted:,},  # ← contagem de shorts publicados no mês")
        print(f"     'longs_views': {longs_views:,},  # ← soma de views de vídeos longos publicados no mês")
        print(f"     'shorts_views': {shorts_views:,},  # ← soma de views de shorts publicados no mês")
        print(f"     'source': 'auto'  # ← indica que foi gerado automaticamente")
        print(f"   }}")
        print()
        
        # Armazena resultado
        results.append({
            'channel_name': channel.name,
            'channel_id': channel.channel_id,
            'year': example_year,
            'month': example_month,
            'views': views_from_metrics,
            'subscribers': subscribers_diff,
            'video_count': video_count_from_metrics,
            'longs_posted': longs_posted,
            'shorts_posted': shorts_posted,
            'longs_views': longs_views,
            'shorts_views': shorts_views
        })
    
    # Resumo final
    print("\n" + "="*100)
    print("RESUMO - DADOS QUE SERIAM INSERIDOS EM historical_metrics")
    print("="*100)
    print()
    print(f"{'Canal':<30} {'Views':>15} {'Subs Δ':>12} {'Videos':>8} {'Longs':>8} {'Shorts':>8} {'L.Views':>12} {'S.Views':>12}")
    print("-" * 100)
    
    for result in results:
        name = result['channel_name'][:28] + ".." if len(result['channel_name']) > 30 else result['channel_name']
        print(f"{name:<30} {result['views']:>15,} {result['subscribers']:>12,} {result['video_count']:>8,} {result['longs_posted']:>8,} {result['shorts_posted']:>8,} {result['longs_views']:>12,} {result['shorts_views']:>12,}")
    
    print()
    print("="*100)
    print("LEGENDA:")
    print("  Views: Valor do último dia do mês (tabela metrics)")
    print("  Subs Δ: Diferença de subscribers (final - inicial) do mês (tabela metrics)")
    print("  Videos: Valor do último dia do mês (tabela metrics)")
    print("  Longs, Shorts: Quantidade de vídeos publicados no mês (tabela videos)")
    print("  L.Views, S.Views: Soma de views dos vídeos publicados no mês (tabela videos)")
    print("="*100)
    
    # Salva em arquivo texto
    output_file = "exemplo_historical_metrics.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("="*100 + "\n")
        f.write("EXEMPLO DE DADOS AGREGADOS PARA historical_metrics\n")
        f.write("="*100 + "\n")
        f.write(f"\nMês de exemplo: {example_month}/{example_year} (MÊS ATUAL)\n")
        f.write(f"Período: {first_day.isoformat()} até {last_day.isoformat()}\n")
        f.write(f"Data atual: {today.isoformat()} (dia {today.day} do mês)\n")
        f.write("\n" + "="*100 + "\n\n")
        f.write("ESTE ARQUIVO MOSTRA COMO FICARIAM OS DADOS APÓS A IMPLEMENTAÇÃO DA FUNCIONALIDADE\n")
        f.write("DE GERAÇÃO AUTOMÁTICA DE HISTORICAL METRICS\n\n")
        f.write("="*100 + "\n\n")
        
        for i, result in enumerate(results, 1):
            f.write(f"\n{'='*100}\n")
            f.write(f"CANAL {i}: {result['channel_name']}\n")
            f.write(f"{'='*100}\n")
            f.write(f"Channel ID: {result['channel_id']}\n\n")
            f.write("DADOS QUE SERIAM INSERIDOS/ATUALIZADOS EM historical_metrics:\n")
            f.write("-" * 100 + "\n")
            f.write(f"  channel_id: '{result['channel_id']}'\n")
            f.write(f"  year: {result['year']}\n")
            f.write(f"  month: {result['month']}\n")
            f.write(f"  views: {result['views']:,}  (do último dia de metrics do mês)\n")
            f.write(f"  subscribers: {result['subscribers']:,}  (diferença: final - inicial do mês)\n")
            f.write(f"  video_count: {result['video_count']:,}  (do último dia de metrics do mês)\n")
            f.write(f"  longs_posted: {result['longs_posted']:,}  (vídeos longos publicados no mês)\n")
            f.write(f"  shorts_posted: {result['shorts_posted']:,}  (shorts publicados no mês)\n")
            f.write(f"  longs_views: {result['longs_views']:,}  (soma views de longs do mês)\n")
            f.write(f"  shorts_views: {result['shorts_views']:,}  (soma views de shorts do mês)\n")
            f.write(f"  source: 'auto'  (gerado automaticamente pela cron job)\n")
            f.write("\n")
        
        f.write("\n" + "="*100 + "\n")
        f.write("RESUMO TABULAR\n")
        f.write("="*100 + "\n\n")
        f.write(f"{'Canal':<30} {'Views':>15} {'Subs Δ':>12} {'Videos':>8} {'Longs':>8} {'Shorts':>8} {'L.Views':>12} {'S.Views':>12}\n")
        f.write("-" * 100 + "\n")
        
        for result in results:
            name = result['channel_name'][:28] + ".." if len(result['channel_name']) > 30 else result['channel_name']
            f.write(f"{name:<30} {result['views']:>15,} {result['subscribers']:>12,} {result['video_count']:>8,} {result['longs_posted']:>8,} {result['shorts_posted']:>8,} {result['longs_views']:>12,} {result['shorts_views']:>12,}\n")
        
        f.write("\n" + "="*100 + "\n")
        f.write("LEGENDA:\n")
        f.write("  Views: Valor do último dia do mês (tabela metrics)\n")
        f.write("  Subs Δ: Diferença de subscribers (final - inicial) do mês (tabela metrics)\n")
        f.write("  Videos: Valor do último dia do mês (tabela metrics)\n")
        f.write("  Longs, Shorts: Quantidade de vídeos publicados no mês (tabela videos)\n")
        f.write("  L.Views, S.Views: Soma de views dos vídeos publicados no mês (tabela videos)\n")
        f.write("\n")
        f.write("NOTAS IMPORTANTES:\n")
        f.write("  - A cron job executaria diariamente e atualizaria apenas o mês atual\n")
        f.write("  - No último dia do mês, após atualizar o mês atual, criaria entradas para o próximo mês\n")
        f.write("  - Os dados são calculados a partir das tabelas 'metrics' (diárias) e 'videos' (publicações)\n")
        f.write("  - O campo 'subscribers' armazena a diferença (crescimento) do mês, não o valor absoluto\n")
        f.write("="*100 + "\n")
    
    print(f"\n✅ Arquivo salvo em: {output_file}")
    print()

if __name__ == "__main__":
    get_example_month_data()

