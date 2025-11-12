# Extrator de Vídeos do YouTube

Sistema completo para extrair vídeos de canais do YouTube e armazenar no Supabase, com interface desktop, agendamento configurável e detecção de Shorts.

## Funcionalidades

- ✅ Extração automática de vídeos de múltiplos canais
- ✅ Busca retroativa gradual (do mais recente para o mais antigo)
- ✅ Busca de vídeos novos automaticamente
- ✅ Detecção automática de Shorts (formato 9:16)
- ✅ Suporte a múltiplas chaves de API com rotação automática
- ✅ Agendamento configurável (1 ou 2 vezes por dia)
- ✅ Interface desktop simples e intuitiva
- ✅ Gestão inteligente de quota da API
- ✅ Priorização de canais

## Instalação

1. Crie um ambiente virtual (se ainda não criou):
```bash
python3 -m venv venv
```

2. Ative o ambiente virtual e instale as dependências:
```bash
source venv/bin/activate
pip install -r requirements.txt
```

3. Configure as credenciais no arquivo `config.py`:
   - URL e chave do Supabase (já configuradas)
   - Chave inicial da API do YouTube (já configurada)

4. Execute o aplicativo:

**Opção A - Interface Gráfica (requer python3-tk instalado):**
```bash
# Instale o tkinter primeiro:
sudo apt install python3-tk

# Depois execute:
./run.sh
# ou
source venv/bin/activate
python main.py
```

**Opção B - Interface de Linha de Comando (CLI):**
```bash
source venv/bin/activate
python main_cli.py
```

A versão CLI funciona sem interface gráfica e oferece um menu interativo.

**Opção C - Modo Daemon (roda em background):**
```bash
# Executa em background (sem interface)
source venv/bin/activate
python run_daemon.py

# Ou usando o script
./run_background.sh
```

**Opção D - Como Serviço do Sistema (systemd):**
```bash
# Copia o arquivo de serviço
sudo cp extrator.service /etc/systemd/system/

# Recarrega systemd
sudo systemctl daemon-reload

# Inicia o serviço
sudo systemctl start extrator

# Habilita para iniciar automaticamente no boot
sudo systemctl enable extrator

# Verifica status
sudo systemctl status extrator

# Para o serviço
sudo systemctl stop extrator

# Ver logs
sudo journalctl -u extrator -f
```

Com systemd, o extrator roda automaticamente mesmo quando você não está logado.

**Opção E - GitHub Actions (na nuvem, gratuito):**
```bash
# Veja o guia completo em GITHUB_ACTIONS_SETUP.md
# Resumo:
# 1. Faça push do código para o GitHub
# 2. Configure os secrets (SUPABASE_URL, SUPABASE_KEY, YOUTUBE_API_KEY)
# 3. O workflow roda automaticamente 1x por dia (3:00 BRT)
```

Com GitHub Actions, o extrator roda automaticamente na nuvem, sem precisar manter sua máquina ligada. Veja `GITHUB_ACTIONS_SETUP.md` para instruções detalhadas.

**Atualização de Canais (GitHub Actions):**
- **Automático:** Executa diariamente às 23:59 BRT atualizando todos os canais
- **Manual com seleção:** Ao executar manualmente o workflow "Atualizar Estatísticas dos Canais", você pode:
  - Deixar o campo `channel_id` vazio para atualizar todos os canais
  - Informar um `channel_id` específico para atualizar apenas aquele canal
  - Exemplo: `UCPX0gLduKAfgr-HJENa7CFw` (ID do canal do YouTube)

## Uso

### Interface Desktop

1. **Configurar Agendamento:**
   - Marque "Habilitar agendamento"
   - Defina os horários (formato HH:MM)
   - Clique em "Salvar Configuração"

2. **Gerenciar Chaves de API:**
   - Adicione múltiplas chaves para evitar limite de quota
   - O sistema rotaciona automaticamente quando uma chave excede a quota

3. **Executar Manualmente:**
   - Clique em "Atualizar Agora" para executar imediatamente
   - Use "Parar" para interromper execução em andamento

### Estratégia de Busca

- **Horário da Tarde (12:00 - 18:00):** Busca vídeos antigos retroativamente
  - Processa canais com `needs_old_videos = TRUE`
  - Busca 50 vídeos por execução (do mais recente para o mais antigo)
  - Continua na próxima execução de onde parou

- **Horário da Noite/Madrugada (18:00 - 12:00):** Busca vídeos novos
  - Processa todos os canais
  - Busca vídeos publicados após `newest_video_date`

## Estrutura do Banco de Dados

### Tabela `channels`
- `channel_id` (TEXT UNIQUE) - ID do canal
- `name` (TEXT) - Nome do canal
- `needs_old_videos` (BOOLEAN) - Flag para buscar vídeos antigos
- `priority` (INTEGER) - Prioridade do canal (1-10)
- `oldest_video_date` (DATE) - Data do vídeo mais antigo coletado
- `newest_video_date` (DATE) - Data do vídeo mais recente coletado
- ... outros campos

### Tabela `videos`
- `video_id` (TEXT UNIQUE) - ID do vídeo
- `channel_id` (TEXT) - ID do canal
- `title` (TEXT) - Título do vídeo
- `format` (TEXT) - Formato: "16:9" ou "9:16"
- `is_short` (BOOLEAN) - Se é Short ou não
- `views`, `likes`, `comments` (INTEGER) - Estatísticas
- `published_at` (TIMESTAMP) - Data de publicação
- `duration` (TEXT) - Duração em formato ISO 8601
- `tags` (TEXT) - Tags em formato JSON
- ... outros campos

### Tabela `metrics`
- `id` (BIGSERIAL PRIMARY KEY) - ID único da métrica
- `channel_id` (TEXT) - ID do canal (foreign key para channels)
- `date` (DATE) - Data da métrica (sem hora)
- `views` (BIGINT) - Total de visualizações do canal na data
- `subscribers` (BIGINT) - Total de inscritos do canal na data
- `video_count` (INTEGER) - Total de vídeos do canal na data
- `created_at` (TIMESTAMP) - Data de criação do registro
- Constraint único: `(channel_id, date)` - garante apenas uma métrica por canal por dia

**Nota:** Esta tabela armazena o histórico diário de métricas dos canais. É atualizada automaticamente pela rotina diária de atualização de canais. Veja `create_metrics_table.sql` para criar a tabela.

## Detecção de Shorts

Um vídeo é identificado como Short se tiver duração menor que 3 minutos (180 segundos).

## Otimizações

- **Paginação completa:** Busca todos os vídeos disponíveis
- **Batch processing:** Processa até 50 vídeos por requisição
- **Cache local:** Armazena informações de playlist para evitar requisições repetidas
- **Gestão de quota:** Monitora e otimiza uso da quota da API
- **Retry automático:** Tenta novamente em caso de erros temporários

## Limites e Configurações

- Quota diária padrão: 10.000 unidades
- Máximo de vídeos por execução: 50 (configurável)
- Delay entre requisições: 0.5s
- Delay entre canais: 0.5s
- Máximo de tentativas de retry: 3

## Segurança

- Credenciais não são versionadas no código
- Chaves de API são armazenadas localmente em arquivo JSON
- Validação de dados antes de inserir no banco

## Troubleshooting

### Erro de Quota Excedida
- Adicione mais chaves de API na interface
- O sistema rotaciona automaticamente entre chaves disponíveis

### Nenhum Vídeo Encontrado
- Verifique se os canais estão cadastrados na tabela `channels`
- Verifique se `needs_old_videos` está configurado corretamente
- Verifique logs para mais detalhes

### Erro de Conexão
- Verifique sua conexão com a internet
- Verifique se as credenciais do Supabase estão corretas
- Verifique se a chave da API do YouTube está válida

