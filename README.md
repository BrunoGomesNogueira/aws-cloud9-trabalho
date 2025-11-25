# 📘 Projeto de Processamento de Dados com PySpark

Este repositório contém um pipeline de processamento de dados
desenvolvido em **Python + PySpark**, estruturado de forma modular
utilizando **Programação Orientada a Objetos (POO)**. O projeto foi
projetado para ser executado em ambientes locais, como **AWS Cloud9**, e
utiliza dados de exemplo (JSON compactado) para demonstrar a leitura,
transformação e gravação em formatos otimizados.

------------------------------------------------------------------------

## 🚀 Funcionalidades Principais

-   📥 Leitura de dados JSON compactados (.json.gz)
-   🔧 Transformações organizadas em classes
-   🧪 Testes automatizados com `pytest`
-   ⚙️ Configuração centralizada via `settings.yaml`
-   🧱 Arquitetura limpa separando sessão Spark, I/O e transformações
-   📤 Gravação dos dados transformados em formatos otimizados (ex.:
    Parquet)


## 🛠️ Pré-requisitos

-   **Python 3.10+**
-   **Java 8/11** (necessário para o Spark)

Instalação dos pacotes após criar o seu virtual environemnt:

``` bash
make install
```

------------------------------------------------------------------------

## ▶️ Como Executar

### 1. Ativar ambiente virtual (opcional)

``` bash
source .venv/bin/activate
```

### Execução manual

```bash
# Executar o pipeline
python src/main.py

# Executar testes
python -m pytest tests/ -v
```

### Arquivos de saída

Os dados processados serão salvos em:

    data/output/


## ⚙️ Configuração

Arquivo de configuração:

    config/settings.yaml

Controla:

-   Caminho dos dados de entrada
-   Caminho dos dados de saída
-   Nome da tabela
-   Parâmetros gerais do pipeline

------------------------------------------------------------------------

## 📋 Comandos Makefile

| Comando | Descrição |
|---------|-----------|
| `make help` | Mostra todos os comandos disponíveis |
| `make install` | Instala dependências do projeto |
| `make test` | Executa testes com pytest (verbose) |
| `make run` | Executa o pipeline principal |
| `make format` | Formata código com black |

------------------------------------------------------------------------

## 🧪 Testes

Execute os testes com:

```bash
make test
```

O comando executa `pytest` com as seguintes opções:
- `-v`: Modo verbose (mostra cada teste e porcentagem)
- `--tb=short`: Traceback curto em caso de erro
- `-ra`: Resumo detalhado ao final

### Executar testes manualmente

```bash
# Teste básico
python -m pytest tests/ -v

# Com cobertura de código
python -m pytest tests/ -v --cov=src --cov-report=html

# Ver tempo de execução de cada teste
python -m pytest tests/ -v --durations=0
```

------------------------------------------------------------------------

## 📦 Estrutura do Projeto

```
.
├── config/
│   └── settings.yaml          # Configurações centralizadas
├── data/
│   ├── input/                 # Dados de entrada
│   └── output/                # Dados processados (gerados)
├── src/
│   ├── config/                # Carregamento de configurações
│   ├── io_utils/              # Leitura e escrita de dados
│   ├── pipeline/              # Pipeline principal
│   ├── processing/            # Transformações
│   ├── session/               # Gerenciamento da sessão Spark
│   └── main.py                # Ponto de entrada
├── tests/
│   └── test_transformations.py
├── Makefile                   # Comandos automatizados
├── pyproject.toml             # Configuração do projeto
└── requirements.txt           # Dependências Python
```



