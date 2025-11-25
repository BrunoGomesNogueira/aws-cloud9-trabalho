"""
Configuração pytest para o projeto.

Este arquivo garante que o diretório src está no PYTHONPATH.
"""

import sys
from pathlib import Path

# Adicionar o diretório src ao PYTHONPATH
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

