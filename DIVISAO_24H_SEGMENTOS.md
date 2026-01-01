# Divisão de 24 Horas por Segmento - Proposta

## 📊 Estrutura Atual vs. Proposta

### ❌ Estrutura Atual
- **5 slots por dia** (1h, 3h, 5h, 7h, 9h BRT)
- **Dias pares**: Fitness (5 slots)
- **Dias ímpares**: Podcast (5 slots)
- **Total**: 10 execuções por dia (5 fitness + 5 podcast)

### ✅ Estrutura Proposta
- **24 slots por dia** (1 slot por hora)
- **Dias pares**: Fitness (24 slots - 00h às 23h BRT)
- **Dias ímpares**: Podcast (24 slots - 00h às 23h BRT)
- **Total**: 24 execuções por dia (12 fitness + 12 podcast alternados)

---

## 🗓️ Calendário de Execução

### Dias Pares (Fitness) - Exemplo: 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30

| Slot | Hora BRT | Hora UTC | Segmento | Descrição |
|------|----------|----------|----------|-----------|
| 0 | 00:00 | 03:00 | Fitness | Meia-noite |
| 1 | 01:00 | 04:00 | Fitness | Madrugada |
| 2 | 02:00 | 05:00 | Fitness | Madrugada |
| 3 | 03:00 | 06:00 | Fitness | Madrugada |
| 4 | 04:00 | 07:00 | Fitness | Madrugada |
| 5 | 05:00 | 08:00 | Fitness | Madrugada |
| 6 | 06:00 | 09:00 | Fitness | Manhã |
| 7 | 07:00 | 10:00 | Fitness | Manhã |
| 8 | 08:00 | 11:00 | Fitness | Manhã |
| 9 | 09:00 | 12:00 | Fitness | Manhã |
| 10 | 10:00 | 13:00 | Fitness | Manhã |
| 11 | 11:00 | 14:00 | Fitness | Manhã |
| 12 | 12:00 | 15:00 | Fitness | Tarde |
| 13 | 13:00 | 16:00 | Fitness | Tarde |
| 14 | 14:00 | 17:00 | Fitness | Tarde |
| 15 | 15:00 | 18:00 | Fitness | Tarde |
| 16 | 16:00 | 19:00 | Fitness | Tarde |
| 17 | 17:00 | 20:00 | Fitness | Tarde |
| 18 | 18:00 | 21:00 | Fitness | Noite |
| 19 | 19:00 | 22:00 | Fitness | Noite |
| 20 | 20:00 | 23:00 | Fitness | Noite |
| 21 | 21:00 | 00:00 | Fitness | Noite |
| 22 | 22:00 | 01:00 | Fitness | Noite |
| 23 | 23:00 | 02:00 | Fitness | Noite |

### Dias Ímpares (Podcast) - Exemplo: 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31

| Slot | Hora BRT | Hora UTC | Segmento | Descrição |
|------|----------|----------|----------|-----------|
| 0 | 00:00 | 03:00 | Podcast | Meia-noite |
| 1 | 01:00 | 04:00 | Podcast | Madrugada |
| 2 | 02:00 | 05:00 | Podcast | Madrugada |
| 3 | 03:00 | 06:00 | Podcast | Madrugada |
| 4 | 04:00 | 07:00 | Podcast | Madrugada |
| 5 | 05:00 | 08:00 | Podcast | Madrugada |
| 6 | 06:00 | 09:00 | Podcast | Manhã |
| 7 | 07:00 | 10:00 | Podcast | Manhã |
| 8 | 08:00 | 11:00 | Podcast | Manhã |
| 9 | 09:00 | 12:00 | Podcast | Manhã |
| 10 | 10:00 | 13:00 | Podcast | Manhã |
| 11 | 11:00 | 14:00 | Podcast | Manhã |
| 12 | 12:00 | 15:00 | Podcast | Tarde |
| 13 | 13:00 | 16:00 | Podcast | Tarde |
| 14 | 14:00 | 17:00 | Podcast | Tarde |
| 15 | 15:00 | 18:00 | Podcast | Tarde |
| 16 | 16:00 | 19:00 | Podcast | Tarde |
| 17 | 17:00 | 20:00 | Podcast | Tarde |
| 18 | 18:00 | 21:00 | Podcast | Noite |
| 19 | 19:00 | 22:00 | Podcast | Noite |
| 20 | 20:00 | 23:00 | Podcast | Noite |
| 21 | 21:00 | 00:00 | Podcast | Noite |
| 22 | 22:00 | 01:00 | Podcast | Noite |
| 23 | 23:00 | 02:00 | Podcast | Noite |

---

## 📅 Exemplo de Execução - Janeiro 2025

| Data | Dia | Segmento | Execuções | Horários |
|------|-----|----------|-----------|----------|
| 01/01 | Ímpar | Podcast | 24 slots | 00:00 às 23:00 BRT |
| 02/01 | Par | Fitness | 24 slots | 00:00 às 23:00 BRT |
| 03/01 | Ímpar | Podcast | 24 slots | 00:00 às 23:00 BRT |
| 04/01 | Par | Fitness | 24 slots | 00:00 às 23:00 BRT |
| ... | ... | ... | ... | ... |

---

## 🔄 Distribuição de Canais

### Exemplo com 100 canais Fitness e 50 canais Podcast

#### Dias Pares (Fitness - 100 canais)
- **Slot 0 (00:00 BRT)**: ~4 canais (100 ÷ 24 = 4.17)
- **Slot 1 (01:00 BRT)**: ~4 canais
- **Slot 2 (02:00 BRT)**: ~4 canais
- ...
- **Slot 23 (23:00 BRT)**: ~4 canais
- **Total**: 100 canais distribuídos em 24 slots

#### Dias Ímpares (Podcast - 50 canais)
- **Slot 0 (00:00 BRT)**: ~2 canais (50 ÷ 24 = 2.08)
- **Slot 1 (01:00 BRT)**: ~2 canais
- **Slot 2 (02:00 BRT)**: ~2 canais
- ...
- **Slot 23 (23:00 BRT)**: ~2 canais
- **Total**: 50 canais distribuídos em 24 slots

---

## ⚙️ Configuração GitHub Actions (Cron)

### Horários em UTC (BRT = UTC-3)

```yaml
schedule:
  # Fitness (Dias pares) - 24 execuções
  - cron: '0 3 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30'  # 00:00 BRT
  - cron: '0 4 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30'  # 01:00 BRT
  - cron: '0 5 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30'  # 02:00 BRT
  - cron: '0 6 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30'  # 03:00 BRT
  - cron: '0 7 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30'  # 04:00 BRT
  - cron: '0 8 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30'  # 05:00 BRT
  - cron: '0 9 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30'  # 06:00 BRT
  - cron: '0 10 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 07:00 BRT
  - cron: '0 11 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 08:00 BRT
  - cron: '0 12 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 09:00 BRT
  - cron: '0 13 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 10:00 BRT
  - cron: '0 14 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 11:00 BRT
  - cron: '0 15 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 12:00 BRT
  - cron: '0 16 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 13:00 BRT
  - cron: '0 17 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 14:00 BRT
  - cron: '0 18 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 15:00 BRT
  - cron: '0 19 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 16:00 BRT
  - cron: '0 20 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 17:00 BRT
  - cron: '0 21 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 18:00 BRT
  - cron: '0 22 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 19:00 BRT
  - cron: '0 23 * * 2,4,6,8,10,12,14,16,18,20,22,24,26,28,30' # 20:00 BRT
  - cron: '0 0 * * 3,5,7,9,11,13,15,17,19,21,23,25,27,29,31'  # 21:00 BRT (próximo dia UTC)
  - cron: '0 1 * * 3,5,7,9,11,13,15,17,19,21,23,25,27,29,31'  # 22:00 BRT
  - cron: '0 2 * * 3,5,7,9,11,13,15,17,19,21,23,25,27,29,31'  # 23:00 BRT
  
  # Podcast (Dias ímpares) - 24 execuções
  - cron: '0 3 * * 1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31' # 00:00 BRT
  - cron: '0 4 * * 1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31' # 01:00 BRT
  # ... (similar para todos os 24 slots)
```

**Nota**: GitHub Actions não suporta especificar dias específicos do mês diretamente no cron. 
A solução será usar `cron: '0 * * * *'` (a cada hora) e determinar segmento/slot no código baseado no dia e hora.

---

## 📝 Resumo da Proposta

### ✅ Vantagens
1. **Cobertura completa**: 24 horas de processamento por segmento
2. **Distribuição uniforme**: Carga distribuída ao longo do dia
3. **Maior frequência**: Atualizações mais frequentes dos vídeos
4. **Menor carga por execução**: Menos canais por slot = execuções mais rápidas

### ⚠️ Considerações
1. **48 execuções por dia**: 24 fitness + 24 podcast (alternados)
2. **Quota da API**: Verificar se há quota suficiente para 48 execuções diárias
3. **Custo**: Mais execuções = mais uso de recursos do GitHub Actions
4. **Complexidade**: Cron do GitHub Actions precisa ser simplificado (usar `0 * * * *` e determinar no código)

---

## 🎯 Próximos Passos

1. ✅ Aprovar esta divisão
2. ⏳ Atualizar código Python para suportar 24 slots
3. ⏳ Atualizar workflow do GitHub Actions
4. ⏳ Testar distribuição de canais
5. ⏳ Monitorar quota e performance

