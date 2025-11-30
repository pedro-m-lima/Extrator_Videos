"""
Script MANUAL para atualizar estatísticas de vídeos (views, likes, comments)
Permite escolher qual slot executar manualmente
Versão para execução manual - usuário escolhe o slot
"""
import sys
import os
from datetime import datetime
from typing import List, Optional, Dict, Set
import config
from api_key_manager import APIKeyManager
from supabase_client import SupabaseClient
from youtube_updater import YouTubeUpdater
from models import Channel


def log(message: str, level: str = "INFO"):
    """Adiciona mensagem aos logs"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = {
        "INFO": "[INFO]",
        "SUCCESS": "[✓]",
        "ERROR": "[✗]",
        "WARNING": "[!]"
    }.get(level, "[INFO]")
    
    print(f"{timestamp} {prefix} {message}")
    sys.stdout.flush()


def get_channels_for_slot(all_channels: List[Channel], slot: int, total_slots: int = 12) -> List[Channel]:
    """
    Obtém canais de um slot específico
    
    Args:
        all_channels: Lista de todos os canais
        slot: Número do slot (0-11)
        total_slots: Número total de slots (padrão 12)
    
    Returns:
        Lista de canais do slot especificado
    """
    if not all_channels:
        return []
    
    if slot < 0 or slot >= total_slots:
        return []
    
    # Calcula quantos canais processar por slot
    channels_per_slot = (len(all_channels) + total_slots - 1) // total_slots  # Arredonda para cima
    
    # Calcula índice inicial e final
    start_idx = slot * channels_per_slot
    end_idx = min(start_idx + channels_per_slot, len(all_channels))
    
    channels_to_process = all_channels[start_idx:end_idx]
    
    return channels_to_process


def display_slots_info(all_channels: List[Channel], total_slots: int = 12):
    """
    Exibe informações sobre os slots disponíveis
    
    Args:
        all_channels: Lista de todos os canais
        total_slots: Número total de slots
    """
    log("=" * 60)
    log("INFORMAÇÕES DOS SLOTS", "INFO")
    log("=" * 60)
    log(f"Total de canais: {len(all_channels)}")
    log(f"Total de slots: {total_slots}")
    
    channels_per_slot = (len(all_channels) + total_slots - 1) // total_slots
    
    log("\nDistribuição dos slots:")
    for slot in range(total_slots):
        start_idx = slot * channels_per_slot
        end_idx = min(start_idx + channels_per_slot, len(all_channels))
        num_channels = end_idx - start_idx
        
        if num_channels > 0:
            log(f"  Slot {slot + 1}: Canais {start_idx + 1} a {end_idx} ({num_channels} canais)")
    
    log("=" * 60)


def parse_slot_input(user_input: str, total_slots: int = 12) -> Set[int]:
    """
    Parse do input do usuário para slots
    
    Args:
        user_input: String com slots separados por vírgula (ex: "1,2,3" ou "1-3")
        total_slots: Número total de slots
    
    Returns:
        Set com números dos slots (0-indexed)
    """
    slots = set()
    
    # Remove espaços
    user_input = user_input.strip()
    
    if not user_input:
        return slots
    
    # Processa cada parte separada por vírgula
    parts = [p.strip() for p in user_input.split(',')]
    
    for part in parts:
        # Verifica se é um range (ex: "1-3")
        if '-' in part:
            try:
                start, end = part.split('-')
                start_slot = int(start.strip()) - 1  # Converte para 0-indexed
                end_slot = int(end.strip())  # Mantém 1-indexed para incluir o último
                
                # Valida range
                if start_slot < 0 or end_slot > total_slots or start_slot >= end_slot:
                    log(f"Range inválido: {part}", "ERROR")
                    continue
                
                # Adiciona todos os slots do range
                for slot in range(start_slot, end_slot):
                    if 0 <= slot < total_slots:
                        slots.add(slot)
            except ValueError:
                log(f"Formato inválido: {part}", "ERROR")
                continue
        else:
            # É um número único
            try:
                slot = int(part) - 1  # Converte para 0-indexed
                if 0 <= slot < total_slots:
                    slots.add(slot)
                else:
                    log(f"Slot {slot + 1} fora do range válido (1-{total_slots})", "ERROR")
            except ValueError:
                log(f"Valor inválido: {part}", "ERROR")
                continue
    
    return slots


def get_user_slot_selection(all_channels: List[Channel], total_slots: int = 12) -> Set[int]:
    """
    Solicita ao usuário qual slot(s) executar
    
    Args:
        all_channels: Lista de todos os canais
        total_slots: Número total de slots
    
    Returns:
        Set com números dos slots selecionados (0-indexed)
    """
    display_slots_info(all_channels, total_slots)
    
    log("\nSelecione o(s) slot(s) para executar:", "INFO")
    log("  - Digite o número do slot (ex: 1, 2, 3)", "INFO")
    log("  - Ou múltiplos slots separados por vírgula (ex: 1,2,3)", "INFO")
    log("  - Ou um range (ex: 1-3 para slots 1, 2 e 3)", "INFO")
    log("  - Ou combine ambos (ex: 1,3,5-7)", "INFO")
    log("  - Digite 'all' para todos os slots", "INFO")
    log("  - Digite 'q' para sair", "INFO")
    
    while True:
        try:
            user_input = input("\nSlot(s): ").strip().lower()
            
            if user_input == 'q' or user_input == 'quit':
                log("Execução cancelada pelo usuário", "WARNING")
                sys.exit(0)
            
            if user_input == 'all':
                return set(range(total_slots))
            
            slots = parse_slot_input(user_input, total_slots)
            
            if not slots:
                log("Nenhum slot válido selecionado. Tente novamente.", "ERROR")
                continue
            
            return slots
            
        except KeyboardInterrupt:
            log("\nExecução cancelada pelo usuário", "WARNING")
            sys.exit(0)
        except Exception as e:
            log(f"Erro ao processar input: {e}", "ERROR")
            continue


def run_update_videos_stats_manual(channel_ids: Optional[List[str]] = None, slots: Optional[List[int]] = None):
    """
    Executa atualização MANUAL de estatísticas de vídeos (views, likes, comments)
    Varre todos os vídeos dos canais e atualiza apenas os que têm mudanças
    VERSÃO MANUAL: Usuário escolhe qual slot executar
    
    Args:
        channel_ids: Lista de IDs de canais para atualizar. Se None, permite escolher slots.
        slots: Lista de slots para executar (0-indexed). Se None, solicita ao usuário.
    """
    try:
        log("=" * 60)
        log("MODO MANUAL: Atualização por slot selecionado", "INFO")
        log("=" * 60)
        
        # Carrega configurações de variáveis de ambiente (GitHub Secrets)
        if os.getenv('SUPABASE_URL'):
            config.SUPABASE_URL = os.getenv('SUPABASE_URL')
        if os.getenv('SUPABASE_KEY'):
            config.SUPABASE_KEY = os.getenv('SUPABASE_KEY')
        
        # Carrega chaves de API de variáveis de ambiente
        api_keys = []
        if os.getenv('YOUTUBE_API_KEY'):
            api_keys.append(os.getenv('YOUTUBE_API_KEY'))
        
        # Suporta múltiplas chaves separadas por vírgula
        if os.getenv('YOUTUBE_API_KEYS'):
            additional_keys = os.getenv('YOUTUBE_API_KEYS').split(',')
            api_keys.extend([k.strip() for k in additional_keys if k.strip()])
        
        if not api_keys:
            log("Nenhuma chave de API configurada nas variáveis de ambiente!", "ERROR")
            return False
        
        # Inicializa componentes
        api_key_manager = APIKeyManager()
        if api_keys:
            api_key_manager.keys = api_keys
            api_key_manager.current_key_index = 0
        
        supabase_client = SupabaseClient()
        youtube_updater = YouTubeUpdater(api_key_manager, supabase_client)
        
        # Verifica se há chaves disponíveis
        if not api_key_manager.has_available_keys():
            log("Nenhuma chave de API disponível!", "ERROR")
            return False
        
        # Busca canais
        if channel_ids:
            log(f"Iniciando atualização MANUAL de vídeos de {len(channel_ids)} canal(is) específico(s)")
            channels = []
            for channel_id in channel_ids:
                channel = supabase_client.get_channel_by_id(channel_id.strip())
                if channel:
                    channels.append(channel)
                else:
                    log(f"Canal não encontrado: {channel_id}", "WARNING")
            
            if not channels:
                log("Nenhum canal válido encontrado", "ERROR")
                return False
            
            # Extrai IDs dos canais
            channel_ids_list = [c.channel_id for c in channels]
        else:
            # Busca todos os canais
            all_channels = supabase_client.get_channels()
            
            if not all_channels:
                log("Nenhum canal encontrado", "ERROR")
                return False
            
            # Se slots foram fornecidos via variável de ambiente, usa eles
            # Senão, verifica se está em ambiente interativo (terminal) ou não (GitHub Actions)
            if slots is not None:
                selected_slots = set(slots)
            else:
                # Verifica se está em ambiente interativo (tem stdin disponível)
                import sys
                is_interactive = sys.stdin.isatty()
                
                if is_interactive:
                    # Ambiente interativo: solicita slots ao usuário
                    selected_slots = get_user_slot_selection(all_channels, total_slots=12)
                else:
                    # Ambiente não interativo (GitHub Actions): usa slot automático baseado na hora
                    from datetime import datetime
                    current_hour = datetime.now().hour
                    slot = (current_hour // 2) % 12
                    selected_slots = {slot}
                    log(f"Ambiente não interativo detectado. Usando slot automático: {slot + 1} (hora {current_hour:02d}:00)", "INFO")
            
            if not selected_slots:
                log("Nenhum slot selecionado", "ERROR")
                return False
            
            # Coleta canais dos slots selecionados
            channels = []
            for slot in sorted(selected_slots):
                slot_channels = get_channels_for_slot(all_channels, slot, total_slots=12)
                channels.extend(slot_channels)
                log(f"Slot {slot + 1}: {len(slot_channels)} canais adicionados")
            
            if not channels:
                log("Nenhum canal encontrado nos slots selecionados", "ERROR")
                return False
            
            log(f"Total de canais a processar: {len(channels)} (slots: {', '.join([str(s+1) for s in sorted(selected_slots)])})")
            
            # Extrai IDs dos canais
            channel_ids_list = [c.channel_id for c in channels]
        
        # Atualiza vídeos de todos os canais selecionados
        log("=" * 60)
        log("Iniciando processamento...", "INFO")
        log("=" * 60)
        
        total_stats = youtube_updater.update_all_channels_videos(channel_ids_list, log_callback=log)
        
        # Resumo final
        log("=" * 60)
        log("ATUALIZAÇÃO MANUAL CONCLUÍDA!", "SUCCESS")
        log("=" * 60)
        log(f"Total de vídeos processados: {total_stats['total']}", "INFO")
        log(f"Atualizados: {total_stats['updated']}", "SUCCESS")
        log(f"Sem mudanças: {total_stats['unchanged']}", "INFO")
        log(f"Erros: {total_stats['errors']}", "ERROR" if total_stats['errors'] > 0 else "INFO")
        log(f"Não encontrados na API: {total_stats['not_found']}", "WARNING" if total_stats['not_found'] > 0 else "INFO")
        
        # Exibe informações de quota
        quota_info = youtube_updater.get_quota_info()
        log(f"Quota da API: {quota_info['used']}/{quota_info['limit']} usada ({quota_info['percentage_used']:.1f}%)", "INFO")
        log(f"Quota restante: {quota_info['remaining']} unidades", "INFO")
        breakdown = quota_info['breakdown']
        if breakdown['videos_list'] > 0:
            log(f"Detalhamento: videos.list={breakdown['videos_list']}", "INFO")
        
        return True
        
    except Exception as e:
        log(f"Erro na atualização: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Verifica se foi especificado canais via variável de ambiente
    channel_ids_env = os.getenv("CHANNEL_IDS")
    slots_env = os.getenv("SLOTS")
    
    channel_ids = None
    slots = None
    
    if channel_ids_env:
        # Suporta múltiplos channel_ids separados por vírgula
        channel_ids = [cid.strip() for cid in channel_ids_env.split(',') if cid.strip()]
        log(f"Modo MANUAL: Atualização de {len(channel_ids)} canal(is) específico(s)", "INFO")
    elif slots_env:
        # Slots fornecidos via variável de ambiente
        try:
            # Converte para lista de inteiros (0-indexed)
            slots = [int(s.strip()) - 1 for s in slots_env.split(',') if s.strip()]
            # Valida slots
            slots = [s for s in slots if 0 <= s < 12]
            if slots:
                log(f"Modo MANUAL: Executando slots {', '.join([str(s+1) for s in slots])}", "INFO")
            else:
                log("Nenhum slot válido fornecido", "ERROR")
                sys.exit(1)
        except ValueError:
            log("Formato inválido para SLOTS. Use números separados por vírgula (ex: 1,2,3)", "ERROR")
            sys.exit(1)
    else:
        log("Modo MANUAL: Seleção interativa de slots", "INFO")
        log("Para especificar slots via variável de ambiente, use: SLOTS=1,2,3 python update_videos_stats_manual.py", "INFO")
        log("Para especificar canais, use: CHANNEL_IDS=id1,id2,id3 python update_videos_stats_manual.py", "INFO")
    
    success = run_update_videos_stats_manual(channel_ids=channel_ids, slots=slots)
    sys.exit(0 if success else 1)

