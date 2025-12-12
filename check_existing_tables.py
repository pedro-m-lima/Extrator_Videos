#!/usr/bin/env python3
"""
Script para verificar quais tabelas já existem no MySQL
"""
import mysql.connector
import config

def check_tables():
    """Verifica tabelas existentes no MySQL"""
    try:
        connection = mysql.connector.connect(
            host=config.MYSQL_HOST,
            port=config.MYSQL_PORT,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            database=config.MYSQL_DATABASE
        )
        
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        
        print("="*60)
        print("TABELAS EXISTENTES NO MYSQL")
        print("="*60)
        for table in sorted(tables):
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  ✅ {table} ({count} registros)")
        
        print(f"\nTotal: {len(tables)} tabelas")
        
        cursor.close()
        connection.close()
        
        return tables
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        return []

if __name__ == "__main__":
    check_tables()

