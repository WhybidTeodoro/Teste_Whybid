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



