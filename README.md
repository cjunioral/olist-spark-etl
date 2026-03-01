# Olist Spark ETL (Bronze & Silver)

Projeto prático de Engenharia de Dados utilizando **PySpark** no **WSL
(Ubuntu)**, aplicando organização em camadas (Bronze e Silver) com o
dataset público Brazilian E-Commerce (Olist).

A proposta foi consolidar fundamentos de ingestão, transformação e
organização de dados em um pipeline simples e funcional.

------------------------------------------------------------------------

## Estrutura em Camadas

### Bronze

-   Leitura dos arquivos CSV do dataset Olist\
-   Inferência automática de schema\
-   Conversão para formato Parquet\
-   Armazenamento em `data/bronze/`

Nesta camada os dados continuam brutos, apenas padronizados em formato
colunar.

------------------------------------------------------------------------

### Silver

-   Leitura dos dados em Parquet da camada Bronze\
-   Remoção de registros inválidos (nulos em colunas-chave)\
-   Remoção de duplicados\
-   Armazenamento em `data/silver/`

Tabelas tratadas:

-   `orders`
-   `order_items`

Aqui os dados passam por uma limpeza básica para garantir integridade
mínima antes de qualquer análise futura.

------------------------------------------------------------------------

## Tecnologias Utilizadas

-   Python\
-   PySpark\
-   WSL (Ubuntu)\
-   Parquet\
-   Git\
-   GitHub

------------------------------------------------------------------------

## Estrutura do Projeto

    olist-spark-etl/
    │
    ├── data/
    │   ├── raw/
    │   ├── bronze/
    │   └── silver/
    │
    ├── src/
    │   ├── bronze.py
    │   └── silver.py
    │
    ├── requirements.txt
    ├── .gitignore
    └── README.md

> A pasta `data/` não é versionada no repositório.

------------------------------------------------------------------------

## Como Executar

### 1. Criar e ativar ambiente virtual

``` bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Inserir os arquivos CSV do dataset em:

    data/raw/olist/

### 3. Executar as camadas

``` bash
python src/bronze.py
python src/silver.py
```

------------------------------------------------------------------------

## Objetivo

Este projeto foi desenvolvido para praticar:

-   Organização de pipeline de dados\
-   Separação em camadas (Bronze → Silver)\
-   Manipulação de dados com PySpark\
-   Boas práticas de estruturação de projeto

------------------------------------------------------------------------

## Autor

Cícero Ramalho\
Estudante de Engenharia da Computação\
Engenharia de Dados
