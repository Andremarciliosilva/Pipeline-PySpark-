## Pipeline — PySpark & PostgreSQL

Este projeto consiste em criar um Pipeline de Engenharia de Dados que simula um ambiente bancário, 
utilizando **dados fictícios**, **PySpark para processamento** e **PostgreSQL como camada analítica**, seguindo boas práticas de arquitetura em camadas (Bronze, Silver e Gold).

---

###  Objetivo do Projeto

Demonstrar, de forma prática, como funciona um pipeline de dados bancário do ponto de vista de **engenharia de dados**:

- ingestão de dados transacionais fictícios
- aplicação de regras de negócio
- validação e padronização de dados
- modelagem analítica
- carga em banco de dados para consumo

O projeto tem foco em **arquitetura, organização e lógica de negócio**, e não em dados reais.

---

### Tecnologias Utilizadas

- Python 3.10.19
- PySpark 4.1.1
- PostgreSQL

---

### Tabelas Existentes no Projeto

<p align="center">
  <img src="docs/pipeline_modelagem.png" alt="Modelagem" width="800"/>
</p>

---

### Arquitetura do Pipeline

<p align="center">
  <img src="docs/pipeline_bancario.png" alt="Arquitetura do Pipeline Bancário" width="800"/>
</p>

---

### Camadas de Dados

#### Bronze
- Dados gerados em arquivos CSV
- Estrutura imutável (append-only)
- Representa a ingestão inicial de dados bancários

Esta camada simula um **Data Lake bancário**.

#### Silver
- Transformações realizadas com **PySpark**
- Aplicação de regras de negócio
- Padronização de colunas
- Remoção de duplicidades
- Validações de qualidade

Camada de dados confiáveis, pronta para modelagem analítica.

#### Gold — Analytics
- Dados modelados para consumo
- Carregamento no **PostgreSQL**

Esta camada representa um **Data Warehouse analítico bancário**.

---

### Conceitos Demonstrados

- Arquitetura em camadas (Bronze, Silver, Gold)
- Processamento distribuído com PySpark
- Regras de negócio aplicadas em dados financeiros
- Separação entre ingestão, tratamento e consumo

---

### Como Executar o Projeto (Resumo)

1. Gerar os dados fictícios em CSV
2. Executar o processamento com PySpark (Bronze → Silver)
3. Criar as tabelas analíticas no PostgreSQL
4. Carregar os dados finais na camada Gold
5. Consultar os dados via SQL

---

## Observações Importantes

- Todos os dados utilizados são **fictícios**
- O PostgreSQL é utilizado como **simulação de um Data Warehouse**
- A arquitetura foi pensada para refletir práticas reais do setor bancário
- O foco do projeto é **engenharia de dados**, não visualização.

---

## 👤 Autor 
André Silva

Projeto desenvolvido para fins de estudos.