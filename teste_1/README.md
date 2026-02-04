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

⚖️ Trade-off técnico — Processamento em memória vs incremental

Foi escolhido o processamento incremental dos arquivos. Pois os arquivos da ANS podem ser grandes e numerosos

Processar tudo em memória poderia causar:

Alto consumo de RAM

Risco de travamento em ambientes simples

O processamento incremental permite:

Ler um arquivo por vez

Liberar memória após cada processamento

Maior estabilidade e escalabilidade

Essa abordagem é mais segura e adequada para volumes de dados variáveis, além de ser uma boa prática em pipelines de dados.