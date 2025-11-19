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

Instale:

-   **Python 3.10+**
-   **Java 8/11** (necessário para o Spark)
-   **PySpark**
-   **pytest** (para testes)

Instalação dos pacotes:

``` bash
pip install -r requirements.txt
```

------------------------------------------------------------------------

## ▶️ Como Executar

### 1. Ativar ambiente virtual (opcional)

``` bash
source .venv/bin/activate
```

### 2. Executar o pipeline

``` bash
python src/main.py
```

### 3. Arquivos de saída serão gerados em:

    data/output/


## ⚙️ Configuração

Arquivo:

    config/settings.yaml

Controla:

-   Caminho dos dados de entrada\
-   Caminho dos dados de saída\
-   Nome da tabela\
-   Parâmetros gerais do pipeline



