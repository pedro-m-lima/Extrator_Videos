"""
Script auxiliar para listar todos os canais disponíveis
Útil para obter os channel_ids para usar no workflow de atualização de vídeos
"""
import os
import sys
import config
from supabase_client import SupabaseClient

def main():
    # Carrega configurações de variáveis de ambiente (se disponível)
    if os.getenv('SUPABASE_URL'):
        config.SUPABASE_URL = os.getenv('SUPABASE_URL')
    if os.getenv('SUPABASE_KEY'):
        config.SUPABASE_KEY = os.getenv('SUPABASE_KEY')
    
    supabase_client = SupabaseClient()
    
    print("="*80)
    print("  CANAIS DISPONÍVEIS")
    print("="*80)
    print()
    
    channels = supabase_client.get_channels()
    
    if not channels:
        print("Nenhum canal encontrado no banco de dados.")
        return
    
    print(f"Total de canais: {len(channels)}\n")
    print(f"{'#':<4} {'Nome':<40} {'Channel ID':<30}")
    print("-"*80)
    
    for i, channel in enumerate(channels, 1):
        name = channel.name[:37] + "..." if len(channel.name) > 40 else channel.name
        print(f"{i:<4} {name:<40} {channel.channel_id}")
    
    print()
    print("="*80)
    print("Para usar no GitHub Actions, copie os Channel IDs separados por vírgula:")
    print("Exemplo: UCPX0gLduKAfgr-HJENa7CFw,UCxxxxx,UCyyyyy")
    print("="*80)

if __name__ == "__main__":
    main()

