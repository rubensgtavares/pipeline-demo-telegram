# utilizar a camda AWSSDKPandas-Python312 na configuração da função Lambda
# ou adicionar a biblioteca manualmente no pacote de implantação 

import os
import json
import logging
from datetime import datetime, timedelta, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

def parse_data(data: dict) -> dict:
    # se quiser manter o mesmo fuso do lambda:
    tzinfo = timezone(offset=timedelta(hours=-3))
    date = datetime.now(tzinfo).strftime('%Y-%m-%d')
    timestamp = datetime.now(tzinfo).strftime('%Y-%m-%d %H:%M:%S')

    parsed_data: dict[str, list] = {}

    # bloco "from" -> informações do usuário
    from_ = data.get("from", {})
    if from_:
        for k in ("id", "is_bot", "first_name"):
            if k in from_:
                parsed_data[f"user_{k}"] = [from_[k]]

    # bloco "chat" -> informações do chat
    chat = data.get("chat", {})
    if chat:
        for k in ("id", "type"):
            if k in chat:
                parsed_data[f"chat_{k}"] = [chat[k]]

    # campos diretos da mensagem
    for k in ("message_id", "date", "text"):
        if k in data:
            parsed_data[k] = [data[k]]

    # garante sempre a coluna text
    if "text" not in parsed_data:
        parsed_data["text"] = [None]

    # contextos adicionais
    parsed_data["context_date"] = [date]
    parsed_data["context_timestamp"] = [timestamp]

    return parsed_data

def lambda_handler(event: dict, context) -> bool:
    """
    Diariamente é executado para compactar as diversas mensagens,
    no formato JSON, do dia anterior, armazenadas no bucket de dados cru, 
    em um único arquivo no formato PARQUET, armazenando-o no bucket 
    de dados enriquecidos.
    """

    # vars de ambiente
    RAW_BUCKET = os.environ['AWS_S3_BUCKET']
    ENRICHED_BUCKET = os.environ['AWS_S3_ENRICHED']

    # vars lógicas
    tzinfo = timezone(offset=timedelta(hours=-3))
    date = (datetime.now(tzinfo) - timedelta(days=1)).strftime('%Y-%m-%d')
    #date = (datetime.now(tzinfo) - timedelta(days=0)).strftime('%Y-%m-%d')
    timestamp = datetime.now(tzinfo).strftime('%Y%m%d%H%M%S%f')

    client = boto3.client('s3')
    table = None

    try:
        response = client.list_objects_v2(
            Bucket=RAW_BUCKET,
            Prefix=f'telegram/context_date={date}'
        )
        
        logging.info(f"Usando RAW_BUCKET = {RAW_BUCKET}")
        logging.info(f"Prefix = telegram/context_date={date}")
        logging.info(f"response.keys() = {list(response.keys())}")
        logging.info(f"Qtd de objetos = {len(response.get('Contents', []))}")

        contents = response.get('Contents', [])
        if not contents:
            logging.info(f"Nenhum arquivo encontrado em {RAW_BUCKET}/telegram/context_date={date}")
            return True  # nada pra fazer, mas sucesso

        for content in contents:
            key = content['Key']
            filename = key.split('/')[-1]
            local_path = f"/tmp/{filename}"

            # baixa o JSON cru
            client.download_file(RAW_BUCKET, key, local_path)

            # lê o arquivo
            with open(local_path, mode='r', encoding='utf8') as fp:
                data = json.load(fp)
            #    if "message" in data:
            # se o arquivo for {"update_id":..., "message": {...}}:
            # deixe essa linha
            #        data = data["message"]
            # se o arquivo já for só a mensagem, remova a linha acima
            if isinstance(data, dict) and "message" in data:
                data = data["message"]

            parsed_data = parse_data(data=data)
            iter_table = pa.Table.from_pydict(mapping=parsed_data)

            if table is not None:
                table = pa.concat_tables([table, iter_table])
            else:
                table = iter_table

        parquet_path = f'/tmp/{timestamp}.parquet'
        pq.write_table(table, parquet_path)

        client.upload_file(
            parquet_path,
            ENRICHED_BUCKET,
            f'telegram/context_date={date}/{timestamp}.parquet'
        )
        logging.info(f"Escrevendo parquet em /tmp/{timestamp}.parquet")
        logging.info(f"Enviando para s3://{ENRICHED_BUCKET}/telegram/context_date={date}/{timestamp}.parquet")


        return True

    except Exception as exc:
        logging.exception("Erro ao processar mensagens do Telegram", exc_info=True)
        return False
