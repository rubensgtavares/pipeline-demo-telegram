import os
import json
import logging
from datetime import datetime, timezone, timedelta

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET = os.environ["AWS_S3_BUCKET"]
TELEGRAM_CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])

s3_client = boto3.client("s3")


def _normalize_updates(payload):
    """
    Aceita vários formatos:
    - dict único com update
    - dict com 'ok' e 'result' (getUpdates bruto)
    - lista de updates
    Retorna sempre uma lista de updates.
    """
    # Caso seja string (ex.: event["body"])
    if isinstance(payload, str):
        payload = json.loads(payload)

    # Caso venha no formato do getUpdates: {"ok": true, "result": [ ... ]}
    if isinstance(payload, dict) and "result" in payload:
        return payload["result"]

    # Caso seja um único update (dict)
    if isinstance(payload, dict):
        return [payload]

    # Caso já seja lista
    if isinstance(payload, list):
        return payload

    # Qualquer outra coisa, retorna lista vazia
    return []


def lambda_handler(event: dict, context: dict) -> dict:
    """
    Recebe mensagens do Telegram via API Gateway,
    filtra pelo chat_id e salva o JSON bruto no S3.
    """

    logger.info("Evento recebido no Lambda:")
    logger.info(json.dumps(event))

    # --- Descobrir de onde vem o JSON ---
    # Se vier do API Gateway, o corpo vem em event["body"]
    if "body" in event:
        payload = event["body"]
    else:
        # Teste direto no console da Lambda
        payload = event

    updates = _normalize_updates(payload)

    tzinfo = timezone(offset=timedelta(hours=-3))
    date_str = datetime.now(tzinfo).strftime("%Y-%m-%d")

    try:
        for upd in updates:
            # Pega a mensagem (pode ser "message" ou "channel_post" em outros tipos)
            message = upd.get("message") or upd.get("channel_post")
            if not message:
                logger.info("Update sem campo 'message', ignorando.")
                continue

            chat = message.get("chat", {})
            chat_id = chat.get("id")

            logger.info(f"chat_id recebido: {chat_id}")
            logger.info(f"chat_id esperado: {TELEGRAM_CHAT_ID}")

            # Filtra pelo grupo/usuário configurado
            if chat_id != TELEGRAM_CHAT_ID:
                logger.info("Chat ID diferente do configurado, ignorando.")
                continue

            # Gera um timestamp único para o arquivo
            timestamp = datetime.now(tzinfo).strftime("%Y%m%d%H%M%S%f")
            filename = f"{timestamp}.json"
            key = f"telegram/context_date={date_str}/{filename}"

            # Salva o update bruto no S3
            body_bytes = json.dumps(upd, ensure_ascii=False).encode("utf-8")

            logger.info(f"Subindo para s3://{BUCKET}/{key}")
            s3_client.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=body_bytes,
                ContentType="application/json; charset=utf-8",
            )

        # Se chegou até aqui, não deu erro
        return {"statusCode": "200"}

    except Exception as exc:
        logger.exception("Erro ao processar e salvar mensagem do Telegram.")
        return {"statusCode": "500"}
