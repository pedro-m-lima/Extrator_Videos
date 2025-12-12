"""
Cliente para interação com MySQL
Compatibilidade: Mantém SupabaseClient como alias para MySQLClient
"""
from mysql_client import MySQLClient

# Alias para compatibilidade - permite usar SupabaseClient = MySQLClient
SupabaseClient = MySQLClient
