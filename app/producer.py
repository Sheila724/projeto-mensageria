import argparse
import json
import os
from datetime import datetime, timezone

import pika


def parse_args():
    parser = argparse.ArgumentParser(description="Envia pedidos de teste para a fila orders no RabbitMQ.")
    parser.add_argument("--count", type=int, default=1, help="Quantidade de pedidos para enviar")
    parser.add_argument("--uuid", default=None, help="UUID fixo para um único pedido")
    parser.add_argument("--uuid-prefix", default="ORD-2026", help="Prefixo para UUID quando count > 1")
    parser.add_argument("--status", default="separated", help="Status do pedido (ex.: created, paid, separated)")
    parser.add_argument("--channel", default="mobile_app", help="Canal do pedido")
    parser.add_argument("--customer-base", type=int, default=7788, help="ID base do cliente")
    parser.add_argument("--product-base", type=int, default=9001, help="ID base do produto")
    parser.add_argument("--unit-price", type=float, default=2500.00, help="Preço unitário")
    parser.add_argument("--quantity", type=int, default=2, help="Quantidade do item")
    return parser.parse_args()


def order_uuid(index: int, args) -> str:
    if args.uuid and args.count == 1:
        return args.uuid
    return f"{args.uuid_prefix}-{index:04d}"


def build_order(index: int, args) -> dict:
    current_uuid = order_uuid(index, args)
    customer_id = args.customer_base + index - 1
    product_id = args.product_base + index - 1

    return {
        "uuid": current_uuid,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "channel": args.channel,
        "status": args.status,
        "customer": {
            "id": customer_id,
            "name": f"Cliente {customer_id}",
            "email": f"cliente{customer_id}@email.com",
            "document": f"000.000.{customer_id % 1000:03d}-00"
        },
        "seller": {
            "id": 55,
            "name": "Tech Store",
            "city": "Sao Paulo",
            "state": "SP"
        },
        "items": [
            {
                "id": 1,
                "product_id": product_id,
                "product_name": f"Produto {product_id}",
                "unit_price": args.unit_price,
                "quantity": args.quantity,
                "category": {
                    "id": "ELEC",
                    "name": "Eletronicos",
                    "sub_category": {
                        "id": "PHONE",
                        "name": "Smartphones"
                    }
                }
            }
        ],
        "shipment": {
            "carrier": "Correios",
            "service": "SEDEX",
            "status": "shipped",
            "tracking_code": f"BR{100000000 + index}"
        },
        "payment": {
            "method": "pix",
            "status": "approved",
            "transaction_id": f"pay_{current_uuid.lower()}"
        },
        "metadata": {
            "source": "producer-script",
            "batch_index": index
        }
    }


def main():
    args = parse_args()

    if args.count < 1:
        raise ValueError("count deve ser >= 1")

    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "admin"),
        os.getenv("RABBITMQ_PASSWORD", "Gabi0510")
    )
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    rabbitmq_port = int(os.getenv("RABBITMQ_PORT", "5672"))

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials)
    )
    channel = connection.channel()
    channel.queue_declare(queue="orders", durable=True)

    sent_uuids = []
    for i in range(1, args.count + 1):
        data = build_order(i, args)
        channel.basic_publish(
            exchange="",
            routing_key="orders",
            body=json.dumps(data),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        sent_uuids.append(data["uuid"])

    connection.close()
    print(f"Pedidos enviados com sucesso: {', '.join(sent_uuids)}")


if __name__ == "__main__":
    main()
