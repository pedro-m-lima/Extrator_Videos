#!/bin/bash
# Script para executar o extrator em background (sem interface)

cd "$(dirname "$0")"
source venv/bin/activate

# Executa em background e redireciona logs
nohup python main_cli.py > extrator.log 2>&1 &

echo "Extrator iniciado em background (PID: $!)"
echo "Logs sendo salvos em: extrator.log"
echo "Para parar: pkill -f main_cli.py"

