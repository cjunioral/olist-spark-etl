# 📦 Olist Data Pipeline - Arquitetura Medallion

Este projeto demonstra a construção de um pipeline de dados distribuído de ponta a ponta, utilizando o dataset público de e-commerce da **Olist**. O foco principal é a transformação de dados brutos em insights de negócio seguindo padrões profissionais de Engenharia de Dados.

## 🚀 Tecnologias e Ferramentas

* **Linguagem:** Python 3.10+
* **Processamento:** **PySpark** (Spark SQL & DataFrames)
* **Orquestração:** **Apache Airflow 2.10** (DAGs, BashOperator)
* **Armazenamento:** Formato Colunar **Parquet**
* **Ambiente:** WSL2 (Ubuntu 22.04) no Windows 11

---

## 🏗️ Arquitetura Medallion

O fluxo de dados foi estruturado em camadas para garantir a qualidade, governança e linhagem da informação:

1.  **Bronze (Raw):** Ingestão dos arquivos CSV originais para Parquet. Os dados são mantidos em seu estado bruto com adição de *timestamps* de processamento.
2.  **Silver (Cleaned):** Etapa de limpeza técnica. Conversão de tipos (strings para datetime/numeric), tratamento de nulos e deduplicação de registros.
3.  **Gold (Business):** Camada de valor agregado. Criação de tabelas de fatos e dimensões (Star Schema) para KPIs de vendas, performance de frete e análise de produtos.

---

## ⚙️ Orquestração com Apache Airflow

Em vez de execuções manuais, o pipeline é gerenciado por uma **DAG (Directed Acyclic Graph)**. Isso permite:
* **Controle de Dependências:** A camada Gold só processa se a Silver for concluída com sucesso.
* **Observabilidade:** Interface visual para monitoramento de falhas e logs em tempo real.
* **Automação:** Agendamento das tarefas de forma estruturada.

---

## 📁 Estrutura do Repositório

```text
.
├── airflow_home/       # Configurações, Logs e DB do Airflow
│   └── dags/           # Definição do Pipeline (olist_dag.py)
├── data/               # Camadas de Dados (Bronze, Silver, Gold)
├── src/                # Scripts de Transformação PySpark
│   ├── bronze.py       # Ingestão Inicial
│   ├── silver.py       # Limpeza e Tipagem
│   └── gold.py         # Regras de Negócio e Agregações
├── venv/               # Virtual Environment Python
├── .gitignore          # Filtro de arquivos sensíveis e temporários
└── README.md           # Documentação do projeto

```

## 🛠️ Como Reproduzir este Projeto
Clone o repositório:

```Bash
git clone [https://github.com/cjunioral/olist-spark-etl.git](https://github.com/cjunioral/olist-spark-etl.git)
cd olist-spark-etl
```

## Configure o Ambiente Virtual:

```Bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
## Inicie o Orquestrador:

```Bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow standalone
```
## Acesse o Dashboard:
```Bash
    Abra localhost:8080 no navegador e ative a DAG olist_spark_medallion.
```
# Desenvolvido por Cicero Junior 💻
Engenheiro de Dados


