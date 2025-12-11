# Historical Metrics - Documentação de Uso

## 📋 Visão Geral

A funcionalidade de **Historical Metrics** agrega automaticamente as métricas diárias (`metrics`) em métricas mensais na tabela `historical_metrics`.

## 🚀 Como Usar

### Execução Standalone

Para executar manualmente a atualização de historical metrics:

```bash
python3 update_historical_metrics.py
```

Este script:
- Processa o mês atual para todos os canais
- No último dia do mês, cria entradas para o próximo mês

### Integração na Cron Job

A funcionalidade já está integrada na cron job principal (`run_github_actions.py`). Ela executa automaticamente após a extração de vídeos.

**Importante**: Se houver erro na atualização de historical metrics, a cron job **não será interrompida**. O erro será logado como warning.

## 📊 Estrutura dos Dados

### Tabela `historical_metrics`

Cada registro contém:
- `channel_id`: ID do canal
- `year`: Ano (ex: 2025)
- `month`: Mês (1-12)
- `views`: Total de views no último dia do mês
- `subscribers`: Diferença de subscribers no mês (final - inicial)
- `video_count`: Total de vídeos no último dia do mês
- `longs_posted`: Quantidade de vídeos longos publicados no mês
- `shorts_posted`: Quantidade de shorts publicados no mês
- `longs_views`: Soma de views dos vídeos longos publicados no mês
- `shorts_views`: Soma de views dos shorts publicados no mês
- `source`: Sempre 'auto' para métricas geradas automaticamente

### Lógica de Agregação

1. **Views, Video_count**: Valores do último dia do mês (snapshot)
2. **Subscribers**: Diferença entre último e primeiro dia do mês
3. **Longs/Shorts**: Contagem e soma de views apenas dos vídeos publicados no mês
4. **Classificação Long/Short**:
   - Prioridade 1: Campo `is_short` (True = short, False = long)
   - Prioridade 2: Duração (> 180s = long, <= 180s = short)

## 🔄 Fluxo de Execução

### Execução Diária

1. Busca todos os canais ativos
2. Para cada canal:
   - Busca métricas diárias do mês atual
   - Busca vídeos publicados no mês atual
   - Calcula agregados
   - Faz UPSERT em `historical_metrics`

### Último Dia do Mês

Após atualizar o mês atual, cria automaticamente entradas para o próximo mês (com valores zerados) para todos os canais.

## 🧪 Testes

Para testar a implementação:

```bash
python3 test_historical_metrics.py
```

Este script testa a agregação com um canal específico sem modificar dados reais.

## 📁 Arquivos

- `historical_metrics_aggregator.py`: Classe principal de agregação
- `update_historical_metrics.py`: Script standalone para execução
- `test_historical_metrics.py`: Script de teste
- `ESTRUTURA_IMPLEMENTACAO_HISTORICAL_METRICS.md`: Documentação técnica completa

## ⚙️ Configuração

A funcionalidade usa as mesmas configurações do Supabase que o resto do sistema:
- `SUPABASE_URL`: URL do projeto Supabase
- `SUPABASE_KEY`: Chave de API do Supabase

Essas variáveis podem ser definidas em:
- Variáveis de ambiente
- Arquivo `config.py`

## 🔍 Verificação

Para verificar se os dados estão sendo gerados corretamente:

```python
from supabase_client import SupabaseClient

client = SupabaseClient()
response = client.client.table('historical_metrics').select('*').order('created_at', desc=True).limit(10).execute()
print(response.data)
```

## 📝 Notas Importantes

1. **Idempotência**: A função pode ser executada múltiplas vezes no mesmo dia sem problemas
2. **Performance**: Processa todos os canais sequencialmente (pode ser otimizado no futuro)
3. **Tratamento de Erros**: Erros são logados mas não interrompem a execução
4. **UPSERT**: Usa INSERT ... ON CONFLICT para evitar duplicatas

## 🐛 Troubleshooting

### Erro: "Nenhuma métrica encontrada"
- Verifique se existem registros na tabela `metrics` para o canal no mês
- A função não cria entradas vazias se não houver dados

### Erro: "Duplicate key"
- Normal se executar múltiplas vezes no mesmo dia
- O UPSERT trata isso automaticamente

### Performance lenta
- Para muitos canais, o processamento pode demorar
- Considere processar em lotes no futuro

