# 📡 Data Pipeline — Telegram Messages (AWS Data Lake)

Este repositório demonstra um pipeline completo de ingestão, transformação e análise de mensagens do Telegram utilizando serviços da AWS.

O projeto utiliza uma arquitetura Data Lake com camadas RAW e ENRICHED, funções Lambda, orquestração com EventBridge, catalogação com Glue e consultas analíticas com Athena.  
A análise final (EDA) é realizada em Python com Pandas, Matplotlib e Seaborn.

Link do Kaggle: https://www.kaggle.com/code/rubensgabrieltavares/data-pipeline-telegram-messages-ebac

---

## 🚀 Arquitetura Geral

### Fluxo dos dados

**Telegram → API Gateway → Lambda RAW → S3 (RAW) → EventBridge → Lambda ENRICHED → S3 (ENRICHED) → Glue Crawler → Athena → Notebook (EDA)**

### Etapas do fluxo

- Telegram envia evento
- API Gateway recebe e repassa para Lambda RAW
- Lambda RAW salva JSON bruto no S3
- EventBridge agenda transformação
- Lambda ENRICHED lê RAW, normaliza e grava Parquet
- Glue Crawler atualiza catálogo
- Athena consulta os dados
- Notebook realiza a EDA

---

## 📁 Estrutura do Data Lake

### RAW Layer (Bronze)

Armazena as mensagens originais em JSON, particionadas por data:

`telegram/context_date=YYYY-MM-DD/arquivo.json`

### ENRICHED Layer (Silver)

Contém os arquivos Parquet normalizados.

#### Schema final

- message_id (bigint)
- user_id (bigint)
- user_is_bot (boolean)
- user_first_name (string)
- chat_id (bigint)
- chat_type (string)
- text (string)
- date (bigint)
- context_date (string, partition)

---

## ⚙️ Componentes do Pipeline

### 1. Telegram (Fonte OLTP)

Origem dos eventos enviados ao webhook.

### 2. API Gateway — Webhook

Recebe a requisição e repassa para a Lambda RAW via Lambda Proxy Integration.

### 3. Lambda RAW (Ingestão)

Responsável por:

- validar chat_id
- estruturar o evento
- salvar JSON bruto no S3 (RAW Layer)

### 4. S3 — RAW Layer

Armazena todos os eventos originais, imutáveis.

### 5. EventBridge — Orquestração

Agenda execuções da transformação em lote.

### 6. Lambda ENRICHED (Transformação)

Executa:

- leitura da RAW Layer
- normalização
- criação do schema
- gravação em Parquet no ENRICHED

### 7. S3 — ENRICHED Layer

Armazena Parquet particionado.

### 8. Glue Crawler

Descobre schema e atualiza o Data Catalog.

### 9. Athena — Query Engine

Permite consultas SQL aos dados enriquecidos.

Exemplo de consulta — mensagens por dia:

````sql
SELECT context_date, COUNT(*) AS total
FROM telegram
GROUP BY context_date
ORDER BY context_date;
````

## 📊 EDA — Análise Exploratória

Foram analisados:

- Volume de mensagens por dia
- Distribuição por horário
- Distribuição por dia da semana
- Top usuários
- Curva de Pareto (80/20)
- Tamanho das mensagens
- Palavras mais frequentes
- Heatmap dia × hora
- Scatter de horário × tamanho

Colunas auxiliares criadas:

- msg_datetime
- msg_date
- msg_hour
- msg_len

---

## 🔍 Principais Insights

- Atividade concentrada em horários específicos
- Forte padrão de Pareto: poucos usuários enviam muitas mensagens
- Mensagens curtas predominam
- Picos de uso bem definidos
- Pipeline garantiu dados consistentes e eficientes para análise

---

# 🧪 Como Reproduzir a Análise

1. Clone o repositório

```bash
git clone https://github.com/rubensgtavares/pipeline-demo-telegram
cd pipeline-demo-telegram
````

2. Instale dependências

```bash
pip install -r requirements.txt
```

3. Execute o notebook

```bash
jupyter notebook
```

👨‍💻 Autor

Rubens Gabriel Tavares

Engenheiro de Computação • Data Engineer • Python & AWS

GitHub: https://github.com/rubensgtavares

Kaggle: https://kaggle.com/rubensgabrieltavares

📄 Licença

Este projeto está sob a licença MIT.
Sinta-se livre para usar, modificar e contribuir.
