# Estrutura de Implementação - Historical Metrics

## 📋 Visão Geral

Este documento descreve a estrutura proposta para implementação da funcionalidade de geração automática de **Historical Metrics** (métricas históricas mensais) na cron job existente.

## 🎯 Objetivo

Criar/atualizar automaticamente a tabela `historical_metrics` que agrega métricas diárias (`metrics`) em métricas mensais, executando diariamente na cron job.

## 📊 Estrutura da Tabela `historical_metrics`

```sql
CREATE TABLE IF NOT EXISTS historical_metrics (
    id BIGSERIAL PRIMARY KEY,
    channel_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month >= 1 AND month <= 12),
    views INTEGER DEFAULT 0,
    subscribers INTEGER DEFAULT 0,
    video_count INTEGER DEFAULT 0,
    longs_posted INTEGER DEFAULT 0,
    shorts_posted INTEGER DEFAULT 0,
    longs_views INTEGER DEFAULT 0,
    shorts_views INTEGER DEFAULT 0,
    source TEXT DEFAULT 'auto' CHECK (source IN ('import', 'manual', 'sync', 'auto')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(channel_id, year, month)
);
```

## 🔄 Lógica de Agregação Mensal

### Para cada canal e mês, as métricas são calculadas assim:

1. **Views**: Valor da última métrica diária do mês (último dia com dados)
2. **Subscribers**: Diferença entre a última e a primeira métrica diária do mês (`subscribers_final - subscribers_inicial`)
3. **Video_count**: Valor da última métrica diária do mês
4. **Longs_posted**: Quantidade de vídeos longos publicados no mês
   - Duração > 180 segundos OU `is_short = 0`
5. **Shorts_posted**: Quantidade de shorts publicados no mês
   - Duração <= 180 segundos OU `is_short = 1`
6. **Longs_views**: Soma das views de todos os vídeos longos publicados no mês
7. **Shorts_views**: Soma das views de todos os shorts publicados no mês

## 🏗️ Estrutura de Implementação Proposta

### 1. Novo Arquivo: `historical_metrics_aggregator.py`

Este arquivo conterá a lógica de agregação de métricas históricas.

```python
"""
Módulo para agregação de métricas históricas mensais
"""

from supabase_client import SupabaseClient
from models import Channel, Video
from utils import parse_iso8601_duration
from datetime import datetime, date
from calendar import monthrange
from typing import Optional, Dict, List
import logging

class HistoricalMetricsAggregator:
    """Classe responsável por agregar métricas mensais"""
    
    def __init__(self, supabase_client: SupabaseClient):
        self.client = supabase_client
        self.logger = logging.getLogger(__name__)
    
    def aggregate_monthly_metrics(
        self, 
        channel_id: str, 
        year: int, 
        month: int
    ) -> Optional[Dict]:
        """
        Agrega métricas mensais para um canal específico
        
        Returns:
            Dict com as métricas agregadas ou None se não houver dados
        """
        # Implementação aqui
        pass
    
    def upsert_historical_metric(
        self, 
        channel_id: str, 
        year: int, 
        month: int, 
        metrics: Dict
    ) -> bool:
        """
        Insere ou atualiza registro em historical_metrics
        
        Returns:
            True se sucesso, False caso contrário
        """
        # Implementação aqui
        pass
    
    def process_current_month(self) -> Dict:
        """
        Processa o mês atual para todos os canais ativos
        
        Returns:
            Dict com estatísticas do processamento
        """
        # Implementação aqui
        pass
    
    def create_next_month_entries(self) -> Dict:
        """
        Cria entradas para o próximo mês (executado no último dia do mês)
        
        Returns:
            Dict com estatísticas da criação
        """
        # Implementação aqui
        pass
```

### 2. Métodos a Adicionar em `supabase_client.py`

```python
def get_monthly_metrics(
    self, 
    channel_id: str, 
    year: int, 
    month: int
) -> Optional[Dict]:
    """
    Busca métricas diárias de um mês específico
    
    Returns:
        Dict com primeira e última métrica do mês
    """
    pass

def get_videos_published_in_month(
    self, 
    channel_id: str, 
    year: int, 
    month: int
) -> List[Video]:
    """
    Busca vídeos publicados em um mês específico
    
    Returns:
        Lista de vídeos publicados no mês
    """
    pass

def upsert_historical_metric(
    self, 
    channel_id: str, 
    year: int, 
    month: int, 
    metrics: Dict
) -> bool:
    """
    Insere ou atualiza registro em historical_metrics usando UPSERT
    
    Returns:
        True se sucesso, False caso contrário
    """
    pass
```

### 3. Integração na Cron Job

#### Opção A: Integrar em `run_github_actions.py`

Adicionar após o processamento dos canais:

```python
# No final de run_extraction() ou em um ponto apropriado
from historical_metrics_aggregator import HistoricalMetricsAggregator

# Após processar canais
aggregator = HistoricalMetricsAggregator(supabase_client)
stats = aggregator.process_current_month()

# Se for último dia do mês, criar entradas do próximo mês
from datetime import date
from calendar import monthrange
today = date.today()
last_day = monthrange(today.year, today.month)[1]

if today.day == last_day:
    aggregator.create_next_month_entries()
```

#### Opção B: Criar script separado `update_historical_metrics.py`

Script independente que pode ser chamado pela cron job:

```python
#!/usr/bin/env python3
"""
Script para atualizar historical_metrics
Pode ser executado diariamente pela cron job
"""

from supabase_client import SupabaseClient
from historical_metrics_aggregator import HistoricalMetricsAggregator
from datetime import date
from calendar import monthrange

def main():
    client = SupabaseClient()
    aggregator = HistoricalMetricsAggregator(client)
    
    # Processa mês atual
    stats = aggregator.process_current_month()
    print(f"Processados {stats['channels_processed']} canais")
    
    # Se for último dia do mês, cria entradas do próximo mês
    today = date.today()
    last_day = monthrange(today.year, today.month)[1]
    
    if today.day == last_day:
        next_month_stats = aggregator.create_next_month_entries()
        print(f"Criadas {next_month_stats['entries_created']} entradas para o próximo mês")

if __name__ == "__main__":
    main()
```

## 🔍 Fluxo de Execução Diária

```
┌─────────────────────────────────────────────────────────┐
│  Cron Job Executa Diariamente                            │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  1. Busca todos os canais ativos                        │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  2. Para cada canal:                                     │
│     - Busca métricas diárias do mês atual                │
│     - Busca vídeos publicados no mês atual              │
│     - Calcula agregados                                 │
│     - Faz UPSERT em historical_metrics                   │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  3. Verifica se é último dia do mês                      │
└──────────────────┬──────────────────────────────────────┘
                   │
        ┌──────────┴──────────┐
        │                     │
        ▼                     ▼
   SIM (último dia)    NÃO (outros dias)
        │                     │
        ▼                     │
┌───────────────────────┐     │
│ 4. Para cada canal:   │     │
│    - Cria entrada     │     │
│      para próximo     │     │
│      mês com valores  │     │
│      zerados          │     │
└───────────────────────┘     │
        │                     │
        └──────────┬──────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  Fim da execução                                         │
└─────────────────────────────────────────────────────────┘
```

## 📝 Detalhes de Implementação

### 1. Função de Agregação de Métricas

```python
def aggregate_monthly_metrics(channel_id, year, month):
    """
    Lógica de agregação:
    
    1. Busca métricas diárias do mês (tabela metrics)
       - Primeira métrica: para calcular subscribers inicial
       - Última métrica: para views, subscribers final, video_count
    
    2. Calcula subscribers: subscribers_final - subscribers_inicial
    
    3. Busca vídeos publicados no mês (tabela videos)
       - Filtra por published_at no mês/ano
       - Classifica como long/short baseado em:
         * is_short = True → short
         * is_short = False → long
         * Se is_short não existe: duration > 180s → long, senão → short
    
    4. Agrega:
       - longs_posted: contagem de longs
       - shorts_posted: contagem de shorts
       - longs_views: soma de views de longs
       - shorts_views: soma de views de shorts
    
    5. Retorna dict com todas as métricas
    """
```

### 2. UPSERT em historical_metrics

```python
def upsert_historical_metric(channel_id, year, month, metrics):
    """
    Usa INSERT ... ON CONFLICT ... UPDATE do PostgreSQL
    
    SQL equivalente:
    INSERT INTO historical_metrics (
        channel_id, year, month, views, subscribers, 
        video_count, longs_posted, shorts_posted,
        longs_views, shorts_views, source, updated_at
    ) VALUES (...)
    ON CONFLICT (channel_id, year, month)
    DO UPDATE SET
        views = EXCLUDED.views,
        subscribers = EXCLUDED.subscribers,
        video_count = EXCLUDED.video_count,
        longs_posted = EXCLUDED.longs_posted,
        shorts_posted = EXCLUDED.shorts_posted,
        longs_views = EXCLUDED.longs_views,
        shorts_views = EXCLUDED.shorts_views,
        source = 'auto',
        updated_at = NOW();
    """
```

### 3. Tratamento de Erros

- **Log de erros**: Registrar erros mas não quebrar execução
- **Idempotência**: Pode rodar múltiplas vezes sem criar duplicatas
- **Performance**: Processar em lotes se houver muitos canais
- **Validação**: Verificar se existem métricas antes de criar entrada

### 4. Verificação de Vídeo Longo vs Short

```python
def is_video_long(video: Video) -> bool:
    """
    Determina se vídeo é longo ou short
    
    Prioridade:
    1. Campo is_short: se existe e é True → short, False → long
    2. Duração: se duration > 180s → long, senão → short
    3. Se não tem duração e não tem is_short → ignora
    """
    if video.is_invalid:
        return None  # Ignora vídeos inválidos
    
    # Verifica campo is_short primeiro
    if hasattr(video, 'is_short') and video.is_short is not None:
        return not video.is_short  # is_short=True → short, is_short=False → long
    
    # Se não tem is_short, verifica duração
    if video.duration:
        duration_seconds = parse_iso8601_duration(video.duration)
        return duration_seconds > 180
    
    return None  # Sem informação suficiente
```

## 🎯 Comportamento da Cron Job

### Execução Diária

1. **Para o mês atual**:
   - Busca todos os canais ativos
   - Para cada canal, verifica se existe entrada em `historical_metrics` para o mês/ano atual
   - Se existir: atualiza com dados agregados até o dia atual
   - Se não existir: cria nova entrada para o mês atual com dados agregados até o dia atual
   - Atualiza `updated_at` e define `source = 'auto'`

2. **No último dia do mês**:
   - Após atualizar o mês atual pela última vez
   - Cria automaticamente uma nova entrada em `historical_metrics` para o próximo mês
   - Para todos os canais ativos
   - Com valores zerados (serão atualizados no próximo mês)

### Fluxo Contínuo

```
Dia 15 de Janeiro:
  → Atualiza historical_metrics para Janeiro/2025 (dados do dia 1 ao 15)

Dia 31 de Janeiro:
  → Atualiza historical_metrics para Janeiro/2025 (dados do dia 1 ao 31)
  → Cria entradas para Fevereiro/2025 (valores zerados)

Dia 1 de Fevereiro:
  → Atualiza historical_metrics para Fevereiro/2025 (dados do dia 1)

Dia 15 de Fevereiro:
  → Atualiza historical_metrics para Fevereiro/2025 (dados do dia 1 ao 15)
```

## ✅ Requisitos de Implementação

- [ ] Criar função/método que agrega métricas mensais de um canal para um mês/ano específico
- [ ] Integrar essa função na cron job existente
- [ ] Garantir que a função seja idempotente (pode rodar múltiplas vezes sem criar duplicatas)
- [ ] Tratar erros adequadamente (log, mas não quebrar a execução da cron job principal)
- [ ] Considerar performance: se houver muitos canais, processar em lotes ou com rate limiting
- [ ] Implementar lógica de criação de entradas do próximo mês no último dia do mês
- [ ] Usar UPSERT (INSERT ... ON CONFLICT ... UPDATE) para evitar duplicatas
- [ ] Validar classificação de vídeo longo vs short (duração > 180s OU is_short)
- [ ] Sempre definir `source = 'auto'` para métricas geradas automaticamente

## 📄 Arquivos a Criar/Modificar

### Novos Arquivos:
1. `historical_metrics_aggregator.py` - Lógica de agregação
2. `update_historical_metrics.py` - Script standalone (opcional)

### Arquivos a Modificar:
1. `supabase_client.py` - Adicionar métodos auxiliares
2. `run_github_actions.py` - Integrar chamada ao agregador (ou criar workflow separado)

## 🔗 Exemplo de Dados

Veja o arquivo `exemplo_historical_metrics.txt` para visualizar como ficariam os dados após a implementação, usando dados reais dos últimos 10 canais do Supabase.

## 📌 Notas Importantes

1. **Subscribers**: Armazena a diferença (crescimento) do mês, não o valor absoluto
2. **Views e Video_count**: Valores do último dia do mês (snapshot)
3. **Longs/Shorts**: Contagem e soma de views apenas dos vídeos publicados no mês
4. **Source**: Sempre 'auto' para métricas geradas automaticamente
5. **Idempotência**: A função pode ser executada múltiplas vezes no mesmo dia sem problemas
6. **Performance**: Considerar processamento em lotes para muitos canais

