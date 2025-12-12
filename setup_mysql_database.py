#!/usr/bin/env python3
"""
Script para criar as tabelas no MySQL
Execute este script uma vez para configurar o banco de dados
"""
import mysql.connector
from mysql.connector import Error
import config

def create_tables():
    """Cria todas as tabelas necessárias no MySQL"""
    connection = None
    cursor = None
    
    try:
        print("Conectando ao MySQL...")
        connection = mysql.connector.connect(
            host=config.MYSQL_HOST,
            port=config.MYSQL_PORT,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            database=config.MYSQL_DATABASE
        )
        
        if connection.is_connected():
            print("✅ Conectado ao MySQL!")
            cursor = connection.cursor()
            
            # Lê o arquivo SQL
            with open('create_mysql_tables.sql', 'r', encoding='utf-8') as f:
                sql_script = f.read()
            
            # Remove comentários de linha (-- comentário)
            lines = []
            for line in sql_script.split('\n'):
                # Remove comentários mas mantém a linha se tiver código antes do comentário
                if '--' in line:
                    line = line[:line.index('--')]
                lines.append(line)
            sql_script = '\n'.join(lines)
            
            # Divide em comandos individuais (separados por ;)
            commands = []
            current_command = []
            for line in sql_script.split('\n'):
                line = line.strip()
                if not line:
                    continue
                current_command.append(line)
                if line.endswith(';'):
                    command = ' '.join(current_command)
                    if command and command != ';':
                        commands.append(command)
                    current_command = []
            
            # Adiciona último comando se não terminou com ;
            if current_command:
                command = ' '.join(current_command)
                if command:
                    commands.append(command)
            
            print(f"\nExecutando {len(commands)} comandos SQL...")
            
            for i, command in enumerate(commands, 1):
                if command and command.strip() != ';':
                    try:
                        cursor.execute(command)
                        print(f"  ✅ Comando {i}/{len(commands)} executado com sucesso")
                    except Error as e:
                        # Ignora erros de "já existe" mas mostra outros
                        error_msg = str(e).lower()
                        if 'already exists' in error_msg or 'duplicate' in error_msg or '1050' in str(e):
                            print(f"  ⚠️  Comando {i}/{len(commands)}: {e} (ignorado - já existe)")
                        else:
                            print(f"  ❌ Erro no comando {i}/{len(commands)}: {e}")
                            print(f"     Comando: {command[:100]}...")
                            raise
            
            connection.commit()
            print("\n✅ Todas as tabelas foram criadas com sucesso!")
            
            # Lista as tabelas criadas
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print(f"\nTabelas no banco de dados ({len(tables)}):")
            for table in tables:
                print(f"  - {table[0]}")
            
    except Error as e:
        print(f"❌ Erro ao conectar ou executar SQL: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("\n✅ Conexão fechada.")
    
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("SETUP DO BANCO DE DADOS MYSQL")
    print("=" * 60)
    print()
    success = create_tables()
    if success:
        print("\n✅ Setup concluído com sucesso!")
    else:
        print("\n❌ Setup falhou. Verifique os erros acima.")

