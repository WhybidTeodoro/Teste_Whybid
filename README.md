# Teste 1 — Integração com API Pública (ANS)

O objetivo do Teste 1 é consumir dados públicos da ANS, processar arquivos contábeis de diferentes formatos e consolidar informações de **Despesas com Eventos / Sinistros** dos **últimos 3 trimestres disponíveis**.

---

## 📌 O que este projeto faz

O pipeline executa os seguintes passos:

1. Acessa a API pública da ANS
2. Identifica os **3 trimestres mais recentes** disponíveis
3. Baixa todos os arquivos ZIP desses trimestres
4. Extrai automaticamente os ZIPs
5. Lê arquivos em diferentes formatos:
   - CSV
   - TXT
   - XLSX
6. Normaliza as colunas dos arquivos
7. Filtra apenas registros de **Despesas com Eventos / Sinistros**
8. Consolida os dados em um único CSV
9. Compacta o CSV final em um arquivo ZIP

---

---

## 🛠️ Requisitos

- Python **3.10+**
- Bibliotecas externas:
  - `requests`
  - `beautifulsoup4`
  - `pandas`

```
Execute o arquivo principal:

python main.py
Durante a execução, o console exibirá mensagens indicando 
cada etapa do pipeline:
(download, extração, leitura, consolidação, etc).

Ao final, o resultado será gerado em:
output/consolidado_despesas.zip

📊 Arquivo final gerado
O CSV consolidado contém as seguintes colunas:

CNPJ

RazaoSocial

Ano

Trimestre

ValorDespesas

Apenas registros válidos de Despesas com Eventos / Sinistros são incluídos.

Valores zerados, negativos ou inválidos são descartados.
```

# ⚖️ Trade-off técnico — Processamento em memória vs incremental

Foi escolhido o processamento incremental dos arquivos. Pois os arquivos da ANS podem ser grandes e numerosos

Processar tudo em memória poderia causar:

Alto consumo de RAM

Risco de travamento em ambientes simples

O processamento incremental permite:

Ler um arquivo por vez

Liberar memória após cada processamento

Maior estabilidade e escalabilidade

Essa abordagem é mais segura e adequada para volumes de dados variáveis, além de ser uma boa prática em pipelines de dados.


# Teste 2 — Transformação e Validação de Dados

Este teste tem como objetivo realizar a **validação**, **enriquecimento** e **agregação** dos dados consolidados no Teste 1, utilizando informações públicas da ANS.

O foco foi manter o código **simples, legível e fácil de explicar**, priorizando clareza sobre complexidade.

---

## ▶️ Como Executar o Teste 2

### Pré-requisitos
- Python 3.10+
- Conexão com a internet (para download do cadastro da ANS)
- Execução prévia do **Teste 1**, gerando o arquivo: teste_1/output/despesas_eventos_sinistros.csv

### Execução

O Teste 2 é executado a partir de um único arquivo:

python teste_2/main.py

Esse comando executa todas as etapas do teste na ordem correta e gera o resultado final compactado.

# 🔹 Passo 2.2 — Enriquecimento dos Dados
## O que foi feito

Download automático do cadastro de operadoras ativas da ANS 

Join entre os dados de despesas e o cadastro usando:

REG_ANS (despesas)

REGISTRO_OPERADORA (cadastro)

Inclusão das colunas:

CNPJ

RazaoSocial

RegistroANS

Modalidade

UF

## ️ ️⚖️️ Trade-offs técnicos

Registros sem correspondência no cadastro

Estratégia: manter o registro com valores "Desconhecido"

Justificativa: evita perda de dados e permite auditoria posterior

CNPJs em notação científica

Estratégia: conversão para string de 14 dígitos usando Decimal

Justificativa: o cadastro da ANS apresenta CNPJs nesse formato

Registros duplicados no cadastro

Estratégia: manter o primeiro registro encontrado

Justificativa: solução simples e adequada para o contexto do teste

# 🔹 Passo 2.1 — Validação dos Dados

## Validações implementadas

CNPJ válido (formato e dígitos verificadores)

Valor de despesa positivo

Razão Social não vazia

### ⚖️ Trade-off técnico (CNPJ inválido)

Estratégia: descartar do dataset final

Registros inválidos são salvos em um arquivo separado (registros_invalidos.csv)

Justificativa:

Mantém o dataset final consistente

Permite análise dos problemas separadamente

# 🔹 Passo 2.3 — Agregação dos Dados
Dados agrupados por RazaoSocial e UF

Métricas calculadas

Total de despesas

Média de despesas por trimestre

Desvio padrão das despesas

Quantidade de trimestres considerados

Ordenação

Ordenação por total de despesas (do maior para o menor)

## ⚖️ Trade-off técnico (processamento e ordenação)

Estratégia: processamento e ordenação em memória

Justificativa:

Volume de dados reduzido

Código mais simples e fácil de manter

Evita complexidade desnecessária para o contexto do teste


# Teste 3 — Banco de Dados e Análise (MySQL)

Este teste tem como objetivo utilizar **SQL (MySQL 8.0)** para modelar tabelas, importar dados de arquivos CSV e realizar **análises analíticas** a partir dos dados gerados nos Testes 1 e 2.

O foco foi aplicar conceitos básicos de banco de dados de forma **simples, organizada e fácil de explicar**, priorizando clareza e consistência dos dados.

---

## ▶️ Como Executar o Teste 3

### Pré-requisitos
- MySQL 8.0 ou superior
- Acesso a uma ferramenta para executar SQL (ex: MySQL Workbench)
- Ter executado:
  - **Teste 1** (gerando `despesas_eventos_sinistros.csv`)
  - **Teste 2** (gerando `despesas_agregadas.csv`)
- Ter o arquivo de cadastro das operadoras (CADOP) da ANS (`Relatorio_cadop.csv`)

---

### Ordem de Execução

Os scripts SQL devem ser executados **nesta ordem**:

1️⃣ **Criar as tabelas**
```sql
01_ddl_mysql.sql
```
2️⃣ Importar os dados dos CSVs
```
02_import_mysql.sql
```
3️⃣ Executar as queries analíticas
````
03_queries_mysql.sql
````

# 🗄️ Modelagem e Importação de Dados

### Estratégia de modelagem (Trade-off — Normalização)

### tabelas normalizadas

Uma tabela para cadastro de operadoras

Uma tabela para despesas consolidadas por trimestre

Uma tabela para despesas já agregadas

### Justificativa:

Evita duplicação de dados cadastrais

Facilita consultas analíticas

Cadastro muda pouco, despesas crescem com o tempo

Tipos de dados (Trade-off técnico)

Valores monetários: DECIMAL(18,2)

Garante precisão (evita erro de ponto flutuante)

Ano e trimestre: SMALLINT

Mais simples que trabalhar com datas completas

Suficiente para as análises solicitadas

Importação e tratamento de inconsistências

Durante a importação dos CSVs, foram tratados casos como:

Campos obrigatórios vazios

Valores numéricos em formato texto

Trimestres inválidos

Encoding diferente entre arquivos (UTF-8 e LATIN1)

### Estratégia adotada:

Importar primeiro em tabelas temporárias (staging)

Inserir nas tabelas finais apenas dados válidos

Registros inconsistentes são descartados para manter a integridade

# 📊 Queries Analíticas Desenvolvidas

### Query 1 — Crescimento percentual

Identifica as 5 operadoras com maior crescimento percentual

Considera apenas operadoras com dados no primeiro e no último trimestre analisado

#### Justificativa do trade-off:

Evita distorções causadas por dados incompletos

### Query 2 — Distribuição por UF

Lista os 5 estados com maior volume total de despesas

Calcula também a média de despesas por operadora em cada UF

### Query 3 — Operadoras acima da média

Conta quantas operadoras tiveram despesas acima da média geral

Condição: em pelo menos 2 dos 3 trimestres analisados

#### Trade-off técnico:

Uso de CTEs (WITH) para deixar a query mais legível

Boa performance com índices simples

# 📝 Considerações Finais

O teste foi desenvolvido pensando em clareza e simplicidade

As decisões técnicas foram feitas considerando o contexto do problema e o volume de dados

O foco foi resolver corretamente o problema, sem complexidade desnecessária
