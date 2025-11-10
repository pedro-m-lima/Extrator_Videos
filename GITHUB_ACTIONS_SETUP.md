# Configuração do GitHub Actions

Este guia explica como configurar o extrator para rodar automaticamente no GitHub Actions (gratuito e na nuvem).

## 📋 Pré-requisitos

1. Conta no GitHub
2. Repositório do projeto no GitHub
3. Chaves de API do YouTube
4. Credenciais do Supabase

## 🚀 Passo a Passo

### 1. Fazer Push do Código para o GitHub

Se ainda não fez, crie um repositório e faça push:

```bash
cd ~/ExtratorVideos
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/SEU_USUARIO/ExtratorVideos.git
git push -u origin main
```

### 2. Configurar Secrets no GitHub

1. Acesse seu repositório no GitHub
2. Vá em **Settings** → **Secrets and variables** → **Actions**
3. Clique em **New repository secret**
4. Adicione os seguintes secrets:

#### Secrets Obrigatórios:

- **`SUPABASE_URL`**: `https://rmhozuzxcytguvluksih.supabase.co`
- **`SUPABASE_KEY`**: `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJtaG96dXp4Y3l0Z3V2bHVrc2loIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI2NDc4NjIsImV4cCI6MjA3ODIyMzg2Mn0.sOOFm246T0sVBVNOOYmyDFmvGKzet2X5rJvwp0o1UAU`
- **`YOUTUBE_API_KEY`**: `AIzaSyCl5dHCtinYrqz5fv_pItVrIWzXLozWVtQ`

#### Secret Opcional (múltiplas chaves):

- **`YOUTUBE_API_KEYS`**: Se tiver múltiplas chaves, separe por vírgula:
  ```
  AIzaSyCl5dHCtinYrqz5fv_pItVrIWzXLozWVtQ,OUTRA_CHAVE_AQUI,TERCEIRA_CHAVE
  ```

### 3. Personalizar Horários (Opcional)

Edite o arquivo `.github/workflows/extract_videos.yml` para ajustar os horários:

```yaml
schedule:
  - cron: '0 14 * * *'  # 14:00 UTC (ajuste conforme necessário)
  - cron: '0 2 * * *'    # 02:00 UTC (ajuste conforme necessário)
```

**Formato Cron**: `minuto hora dia mês dia-da-semana`
- `0 14 * * *` = Todo dia às 14:00 UTC
- `0 2 * * *` = Todo dia às 02:00 UTC

**Fuso Horário**: GitHub Actions usa UTC. Para converter:
- BRT (UTC-3): Subtraia 3 horas
- BRST (UTC-2): Subtraia 2 horas

Exemplos:
- 14:00 UTC = 11:00 BRT / 12:00 BRST
- 02:00 UTC = 23:00 BRT (dia anterior) / 00:00 BRST

### 4. Testar Manualmente

1. Vá em **Actions** no seu repositório
2. Clique em **Extrair Vídeos do YouTube**
3. Clique em **Run workflow** → **Run workflow**
4. Aguarde a execução e verifique os logs

### 5. Verificar Execuções

- Acesse **Actions** no GitHub para ver histórico
- Clique em cada execução para ver logs detalhados
- Se houver erro, os logs serão salvos como artefato

## ⚙️ Configurações Avançadas

### Executar Mais de 2 Vezes por Dia

Adicione mais entradas no `schedule`:

```yaml
schedule:
  - cron: '0 8 * * *'   # 08:00 UTC
  - cron: '0 14 * * *'  # 14:00 UTC
  - cron: '0 20 * * *'  # 20:00 UTC
  - cron: '0 2 * * *'   # 02:00 UTC
```

### Executar Apenas em Dias Específicos

```yaml
schedule:
  - cron: '0 14 * * 1-5'  # Apenas segunda a sexta
  - cron: '0 2 * * 0'      # Apenas domingos
```

### Notificações por Email

O GitHub envia emails automaticamente quando:
- Workflow falha
- Workflow é cancelado
- Workflow é bem-sucedido (pode desabilitar nas configurações)

## 📊 Limites do GitHub Actions

- **Gratuito**: 2000 minutos/mês
- **Tempo de execução**: ~2-5 minutos por execução
- **Cálculo**: ~400-1000 execuções/mês (gratuito)

## 🔧 Troubleshooting

### Workflow não executa automaticamente

- Verifique se o repositório não está privado (planos gratuitos têm limitações)
- Verifique se o cron está correto
- GitHub pode atrasar até 15 minutos

### Erro de autenticação

- Verifique se os secrets estão configurados corretamente
- Verifique se não há espaços extras nos secrets

### Quota da API esgotada

- Adicione mais chaves de API no secret `YOUTUBE_API_KEYS`
- Ajuste `MAX_VIDEOS_PER_EXECUTION` no `config.py`

### Erro ao instalar dependências

- Verifique se `requirements.txt` está atualizado
- Verifique se a versão do Python está correta (3.11)

## 🎯 Vantagens do GitHub Actions

✅ **Gratuito** para uso pessoal  
✅ **Automático** - não precisa manter máquina ligada  
✅ **Confiável** - infraestrutura do GitHub  
✅ **Logs** - histórico completo de execuções  
✅ **Notificações** - emails automáticos  
✅ **Flexível** - fácil ajustar horários  

## 📝 Notas Importantes

- O GitHub Actions roda em **UTC**, ajuste os horários conforme seu fuso
- Execuções podem ter atraso de até 15 minutos
- Para repositórios privados, há limites no plano gratuito
- Logs são mantidos por 90 dias (gratuito)

