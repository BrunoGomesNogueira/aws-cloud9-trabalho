.PHONY: help install test tests run clean build lint format

help:
	@echo "📘 Comandos disponíveis:"
	@echo ""
	@echo "  make install    - Instala dependências do projeto"
	@echo "  make test       - Executa testes com pytest"
	@echo "  make run        - Executa o pipeline principal"
	@echo "  make clean      - Remove arquivos temporários"
	@echo "  make build      - Cria pacote wheel"
	@echo "  make format     - Formata código com black"
	@echo ""

install:
	@echo "📦 Instalando dependências..."
	pip install --upgrade pip
	pip install -r requirements.txt
	@echo "✅ Dependências instaladas!"

test:
	@echo "🧪 Executando testes..."
	python -m pytest tests/ -v --tb=short -ra
	@echo "✅ Testes concluídos!"

run:
	@echo "🚀 Executando pipeline..."
	python src/main.py
	@echo "✅ Pipeline executado!"


format:
	@echo "✨ Formatando código..."
	python -m black src/ tests/ --line-length=120
	@echo "✅ Formatação concluída!"
