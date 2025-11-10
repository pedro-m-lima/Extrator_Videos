#!/bin/bash
# Script para executar o Extrator de Vídeos do YouTube

cd "$(dirname "$0")"
source venv/bin/activate
python main.py

