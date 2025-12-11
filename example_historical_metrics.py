#!/usr/bin/env python3
"""
Script para gerar exemplo de dados agregados para historical_metrics
Mostra como ficariam os dados para os 10 primeiros canais
"""

from supabase_client import SupabaseClient
from models import Channel, Video
from utils import parse_iso8601_duration
from datetime import datetime, date
from calendar import monthrange

def get_example_month_data():
    """Gera exemplo de dados agregados para um mês específico"""
    client = SupabaseClient()
    
    # Usa outubro de 2025 como exemplo (mês que aparece nos dados existentes)
    example_year = 2025
    example_month = 10
    
    print("="*100)
    print("EXEMPLO DE DADOS AGREGADOS PARA historical_metrics")
    print("="*100)
    print(f"\nMês de exemplo: {example_month}/{example_year}")
    print("="*100)
    print()
    
    # Busca todos os canais
    all_channels = client.get_channels()
    
    # Pega os 10 primeiros canais
    channels_to_process = all_channels[:10]
    
    print(f"Processando {len(channels_to_process)} canais como exemplo...\n")
    
    # Calcula range de datas do mês
    first_day = date(example_year, example_month, 1)
    last_day = date(example_year, example_month, monthrange(example_year, example_month)[1])
    
    print(f"Período: {first_day.isoformat()} até {last_day.isoformat()}\n")
    print("="*100)
    print()
    
    results = []
    
    for i, channel in enumerate(channels_to_process, 1):
        print(f"\n{'='*100}")
        print(f"CANAL {i}/{len(channels_to_process)}: {channel.name}")
        print(f"{'='*100}")
        print(f"Channel ID: {channel.channel_id}")
        print()
        
        # 1. Busca dados de metrics (último dia do mês)
        print("1. DADOS DE 'metrics' (último dia do mês):")
        print("-" * 100)
        try:
            # Busca métrica do último dia do mês
            metrics_response = client.client.table('metrics').select('*').eq('channel_id', channel.channel_id).eq('date', last_day.isoformat()).execute()
            
            if metrics_response.data:
                metric = metrics_response.data[0]
                views_from_metrics = metric.get('views', 0)
                subscribers_from_metrics = metric.get('subscribers', 0)
                video_count_from_metrics = metric.get('video_count', 0)
                
                print(f"   ✅ Encontrado registro para {last_day.isoformat()}")
                print(f"      Views: {views_from_metrics:,}")
                print(f"      Subscribers: {subscribers_from_metrics:,}")
                print(f"      Video Count: {video_count_from_metrics:,}")
            else:
                # Se não tem no último dia, busca o mais próximo antes
                print(f"   ⚠️  Não encontrado para {last_day.isoformat()}, buscando último registro do mês...")
                metrics_all = client.client.table('metrics').select('*').eq('channel_id', channel.channel_id).gte('date', first_day.isoformat()).lte('date', last_day.isoformat()).order('date', desc=True).limit(1).execute()
                
                if metrics_all.data:
                    metric = metrics_all.data[0]
                    views_from_metrics = metric.get('views', 0)
                    subscribers_from_metrics = metric.get('subscribers', 0)
                    video_count_from_metrics = metric.get('video_count', 0)
                    metric_date = metric.get('date')
                    print(f"   ✅ Encontrado registro mais recente: {metric_date}")
                    print(f"      Views: {views_from_metrics:,}")
                    print(f"      Subscribers: {subscribers_from_metrics:,}")
                    print(f"      Video Count: {video_count_from_metrics:,}")
                else:
                    print(f"   ❌ Nenhum registro encontrado no mês")
                    views_from_metrics = 0
                    subscribers_from_metrics = 0
                    video_count_from_metrics = 0
        except Exception as e:
            print(f"   ❌ Erro ao buscar metrics: {e}")
            views_from_metrics = 0
            subscribers_from_metrics = 0
            video_count_from_metrics = 0
        
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
                        pub_date = datetime.fromisoformat(video.published_at.replace('Z', '+00:00'))
                        if pub_date.year == example_year and pub_date.month == example_month:
                            videos_in_month.append(video)
                    except:
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
                
                if not video.duration:
                    # Sem duração, não conta
                    continue
                
                duration_seconds = parse_iso8601_duration(video.duration)
                
                if duration_seconds >= 180:  # >= 3 minutos = vídeo longo
                    longs_posted += 1
                    longs_views += views
                elif duration_seconds > 0:  # > 0 e < 3 minutos = short
                    shorts_posted += 1
                    shorts_views += views
            
            print(f"   📊 Agregados calculados:")
            print(f"      Longs postados: {longs_posted:,}")
            print(f"      Shorts postados: {shorts_posted:,}")
            print(f"      Views de longs: {longs_views:,}")
            print(f"      Views de shorts: {shorts_views:,}")
            
        except Exception as e:
            print(f"   ❌ Erro ao buscar vídeos: {e}")
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
        print(f"     'subscribers': {subscribers_from_metrics:,},  # ← do último dia de metrics")
        print(f"     'video_count': {video_count_from_metrics:,},  # ← do último dia de metrics")
        print(f"     'longs_posted': {longs_posted:,},  # ← contagem de vídeos longos publicados no mês")
        print(f"     'shorts_posted': {shorts_posted:,},  # ← contagem de shorts publicados no mês")
        print(f"     'longs_views': {longs_views:,},  # ← soma de views de vídeos longos publicados no mês")
        print(f"     'shorts_views': {shorts_views:,},  # ← soma de views de shorts publicados no mês")
        print(f"     'source': 'aggregated'  # ← indica que foi calculado automaticamente")
        print(f"   }}")
        print()
        
        # Armazena resultado
        results.append({
            'channel_name': channel.name,
            'channel_id': channel.channel_id,
            'year': example_year,
            'month': example_month,
            'views': views_from_metrics,
            'subscribers': subscribers_from_metrics,
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
    print(f"{'Canal':<30} {'Views':>15} {'Subs':>12} {'Videos':>8} {'Longs':>8} {'Shorts':>8} {'L.Views':>12} {'S.Views':>12}")
    print("-" * 100)
    
    for result in results:
        name = result['channel_name'][:28] + ".." if len(result['channel_name']) > 30 else result['channel_name']
        print(f"{name:<30} {result['views']:>15,} {result['subscribers']:>12,} {result['video_count']:>8,} {result['longs_posted']:>8,} {result['shorts_posted']:>8,} {result['longs_views']:>12,} {result['shorts_views']:>12,}")
    
    print()
    print("="*100)
    print("LEGENDA:")
    print("  Views, Subs, Videos: Valores do último dia do mês (tabela metrics)")
    print("  Longs, Shorts: Quantidade de vídeos publicados no mês (tabela videos)")
    print("  L.Views, S.Views: Soma de views dos vídeos publicados no mês (tabela videos)")
    print("="*100)
    
    # Salva em arquivo texto
    output_file = "exemplo_historical_metrics.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("="*100 + "\n")
        f.write("EXEMPLO DE DADOS AGREGADOS PARA historical_metrics\n")
        f.write("="*100 + "\n")
        f.write(f"\nMês de exemplo: {example_month}/{example_year}\n")
        f.write(f"Período: {first_day.isoformat()} até {last_day.isoformat()}\n")
        f.write("\n" + "="*100 + "\n\n")
        
        for i, result in enumerate(results, 1):
            f.write(f"\n{'='*100}\n")
            f.write(f"CANAL {i}: {result['channel_name']}\n")
            f.write(f"{'='*100}\n")
            f.write(f"Channel ID: {result['channel_id']}\n\n")
            f.write("DADOS QUE SERIAM INSERIDOS:\n")
            f.write("-" * 100 + "\n")
            f.write(f"  channel_id: '{result['channel_id']}'\n")
            f.write(f"  year: {result['year']}\n")
            f.write(f"  month: {result['month']}\n")
            f.write(f"  views: {result['views']:,}  (do último dia de metrics)\n")
            f.write(f"  subscribers: {result['subscribers']:,}  (do último dia de metrics)\n")
            f.write(f"  video_count: {result['video_count']:,}  (do último dia de metrics)\n")
            f.write(f"  longs_posted: {result['longs_posted']:,}  (vídeos longos publicados no mês)\n")
            f.write(f"  shorts_posted: {result['shorts_posted']:,}  (shorts publicados no mês)\n")
            f.write(f"  longs_views: {result['longs_views']:,}  (soma views de longs do mês)\n")
            f.write(f"  shorts_views: {result['shorts_views']:,}  (soma views de shorts do mês)\n")
            f.write(f"  source: 'aggregated'\n")
            f.write("\n")
        
        f.write("\n" + "="*100 + "\n")
        f.write("RESUMO TABULAR\n")
        f.write("="*100 + "\n\n")
        f.write(f"{'Canal':<30} {'Views':>15} {'Subs':>12} {'Videos':>8} {'Longs':>8} {'Shorts':>8} {'L.Views':>12} {'S.Views':>12}\n")
        f.write("-" * 100 + "\n")
        
        for result in results:
            name = result['channel_name'][:28] + ".." if len(result['channel_name']) > 30 else result['channel_name']
            f.write(f"{name:<30} {result['views']:>15,} {result['subscribers']:>12,} {result['video_count']:>8,} {result['longs_posted']:>8,} {result['shorts_posted']:>8,} {result['longs_views']:>12,} {result['shorts_views']:>12,}\n")
        
        f.write("\n" + "="*100 + "\n")
        f.write("LEGENDA:\n")
        f.write("  Views, Subs, Videos: Valores do último dia do mês (tabela metrics)\n")
        f.write("  Longs, Shorts: Quantidade de vídeos publicados no mês (tabela videos)\n")
        f.write("  L.Views, S.Views: Soma de views dos vídeos publicados no mês (tabela videos)\n")
        f.write("="*100 + "\n")
    
    print(f"\n✅ Arquivo salvo em: {output_file}")
    print()

if __name__ == "__main__":
    get_example_month_data()

