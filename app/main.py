import json
import os
import threading
from datetime import datetime
from fastapi import FastAPI, Query, Path
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text
import pika
import traceback
import uvicorn

app = FastAPI(
    title="API Mensageria - FATEC",
    description="API para consulta de pedidos de marketplace consumidos via RabbitMQ.",
    version="1.0.0"
)

engine = create_engine(
    os.getenv("DB_URL", "postgresql+psycopg2://postgres:Gabi0510@localhost:5432/mensageria")
)
credentials = pika.PlainCredentials('admin', 'Gabi0510')

# ===================== CONSUMER =====================
def callback(ch, method, properties, body):
    print("[CONSUMER] Mensagem recebida do RabbitMQ!")
    try:
        data = json.loads(body.decode())
        uuid = data.get("uuid", "SEM-UUID")
        print(f"Processando pedido: {uuid}")

        total_pedido = sum(item["unit_price"] * item["quantity"] for item in data.get("items", []))
        for item in data.get("items", []):
            item["total"] = round(item["unit_price"] * item["quantity"], 2)
        data["total"] = round(total_pedido, 2)

        with engine.connect() as conn:
            # Cliente
            conn.execute(text("""
                INSERT INTO cliente (id, nome, email, documento)
                VALUES (:id, :nome, :email, :documento)
                ON CONFLICT (id) DO NOTHING
            """), {
                "id": data["customer"]["id"],
                "nome": data["customer"]["name"],
                "email": data["customer"]["email"],
                "documento": data["customer"]["document"]
            })

            # Produto
            for item in data.get("items", []):
                conn.execute(text("""
                    INSERT INTO produto (id, nome, categoria)
                    VALUES (:id, :nome, :categoria)
                    ON CONFLICT (id) DO NOTHING
                """), {
                    "id": item["product_id"],
                    "nome": item["product_name"],
                    "categoria": item["category"]["name"]
                })

            # Pedido
            conn.execute(text("""
                INSERT INTO pedido (uuid, created_at, indexed_at, channel, total, status, cliente_id,
                seller_id, seller_nome, seller_cidade, seller_estado, shipment_carrier,
                shipment_service, shipment_status, shipment_tracking, payment_method,
                payment_status, payment_transaction_id, metadata)
                VALUES (:uuid, :created_at, NOW(), :channel, :total, :status, :cliente_id,
                :seller_id, :seller_nome, :seller_cidade, :seller_estado, :shipment_carrier,
                :shipment_service, :shipment_status, :shipment_tracking, :payment_method,
                :payment_status, :payment_transaction_id, :metadata)
                ON CONFLICT (uuid) DO NOTHING
            """), {
                "uuid": uuid,
                "created_at": datetime.fromisoformat(data["created_at"].replace("Z", "+00:00")),
                "channel": data.get("channel"),
                "total": data["total"],
                "status": data["status"],
                "cliente_id": data["customer"]["id"],
                "seller_id": data["seller"]["id"],
                "seller_nome": data["seller"]["name"],
                "seller_cidade": data["seller"]["city"],
                "seller_estado": data["seller"]["state"],
                "shipment_carrier": data["shipment"]["carrier"],
                "shipment_service": data["shipment"]["service"],
                "shipment_status": data["shipment"]["status"],
                "shipment_tracking": data["shipment"]["tracking_code"],
                "payment_method": data["payment"]["method"],
                "payment_status": data["payment"]["status"],
                "payment_transaction_id": data["payment"]["transaction_id"],
                "metadata": json.dumps(data.get("metadata", {}))
            })

            # Itens
            for item in data.get("items", []):
                conn.execute(text("""
                    INSERT INTO item_pedido (pedido_uuid, product_id, product_name, unit_price,
                    quantity, total_item, categoria_id, categoria_nome, subcategoria_id, subcategoria_nome)
                    VALUES (:pedido_uuid, :product_id, :product_name, :unit_price, :quantity,
                    :total_item, :categoria_id, :categoria_nome, :subcategoria_id, :subcategoria_nome)
                    ON CONFLICT (pedido_uuid, product_id) DO NOTHING
                """), {
                    "pedido_uuid": uuid,
                    "product_id": item["product_id"],
                    "product_name": item["product_name"],
                    "unit_price": item["unit_price"],
                    "quantity": item["quantity"],
                    "total_item": item.get("total"),
                    "categoria_id": item["category"]["id"],
                    "categoria_nome": item["category"]["name"],
                    "subcategoria_id": item["category"]["sub_category"]["id"],
                    "subcategoria_nome": item["category"]["sub_category"]["name"]
                })

            conn.commit()
            print(f"Pedido {uuid} salvo no banco com sucesso!")

    except Exception as e:
        print(f"ERRO no callback: {e}")
        print(traceback.format_exc())


def start_consumer():
    try:
        print("Iniciando conexão com RabbitMQ...")
        rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials)
        )
        channel = connection.channel()
        channel.queue_declare(queue='orders', durable=True)
        channel.basic_consume(queue='orders', on_message_callback=callback, auto_ack=True)
        print("🐰 Consumer RabbitMQ iniciado e escutando...")
        channel.start_consuming()
    except Exception as e:
        print(f"Falha ao iniciar consumer: {e}")


# ===================== API =====================
VALID_ORDER_STATUS = {"created", "paid", "separated", "shipped", "delivered", "canceled"}
VALID_ORDER_SORT_FIELDS = {"created_at": "p.created_at", "total": "p.total", "status": "p.status"}
VALID_SORT_ORDER = {"asc", "desc"}


def _format_datetime(dt: datetime) -> str:
    if dt.tzinfo:
        return dt.isoformat().replace("+00:00", "Z")
    return dt.isoformat() + "Z"


def _normalize_metadata(metadata_value):
    if isinstance(metadata_value, dict):
        return metadata_value
    if isinstance(metadata_value, str):
        try:
            return json.loads(metadata_value)
        except json.JSONDecodeError:
            return {}
    return {}


def _build_order_payload(conn, pedido_row):
    pedido = dict(pedido_row._mapping)

    cliente_row = conn.execute(text("""
        SELECT id, nome, email, documento
        FROM cliente
        WHERE id = :cliente_id
    """), {"cliente_id": pedido["cliente_id"]}).fetchone()
    cliente = dict(cliente_row._mapping) if cliente_row else None

    itens = conn.execute(text("""
        SELECT *
        FROM item_pedido
        WHERE pedido_uuid = :uuid
        ORDER BY id
    """), {"uuid": pedido["uuid"]}).fetchall()

    items_payload = []
    total_pedido = 0.0

    for item in itens:
        item_dict = dict(item._mapping)
        total_item = round(float(item_dict["unit_price"]) * int(item_dict["quantity"]), 2)
        total_pedido += total_item

        items_payload.append({
            "id": item_dict["id"],
            "product_id": item_dict["product_id"],
            "product_name": item_dict["product_name"],
            "unit_price": float(item_dict["unit_price"]),
            "quantity": item_dict["quantity"],
            "category": {
                "id": item_dict["categoria_id"],
                "name": item_dict["categoria_nome"],
                "sub_category": {
                    "id": item_dict["subcategoria_id"],
                    "name": item_dict["subcategoria_nome"]
                }
            },
            "total": total_item
        })

    payload = {
        "uuid": pedido["uuid"],
        "created_at": _format_datetime(pedido["created_at"]),
        "channel": pedido["channel"],
        "total": round(total_pedido, 2),
        "status": pedido["status"],
        "customer": {
            "id": cliente["id"] if cliente else pedido["cliente_id"],
            "name": cliente["nome"] if cliente else None,
            "email": cliente["email"] if cliente else None,
            "document": cliente["documento"] if cliente else None
        },
        "seller": {
            "id": pedido["seller_id"],
            "name": pedido["seller_nome"],
            "city": pedido["seller_cidade"],
            "state": pedido["seller_estado"]
        },
        "items": items_payload,
        "shipment": {
            "carrier": pedido["shipment_carrier"],
            "service": pedido["shipment_service"],
            "status": pedido["shipment_status"],
            "tracking_code": pedido["shipment_tracking"]
        },
        "payment": {
            "method": pedido["payment_method"],
            "status": pedido["payment_status"],
            "transaction_id": pedido["payment_transaction_id"]
        },
        "metadata": _normalize_metadata(pedido.get("metadata"))
    }

    return payload


def _validate_status(status: str | None):
    if status is None:
        return None

    normalized_status = status.lower()
    if normalized_status not in VALID_ORDER_STATUS:
        raise ValueError(
            f"Status inválido: {status}. Valores permitidos: {', '.join(sorted(VALID_ORDER_STATUS))}"
        )
    return normalized_status


def _validate_sort(sort_by: str, sort_order: str):
    if sort_by not in VALID_ORDER_SORT_FIELDS:
        raise ValueError(
            f"sortBy inválido: {sort_by}. Valores permitidos: {', '.join(sorted(VALID_ORDER_SORT_FIELDS.keys()))}"
        )

    normalized_order = sort_order.lower()
    if normalized_order not in VALID_SORT_ORDER:
        raise ValueError(
            f"sortOrder inválido: {sort_order}. Valores permitidos: {', '.join(sorted(VALID_SORT_ORDER))}"
        )

    return VALID_ORDER_SORT_FIELDS[sort_by], normalized_order


@app.get(
    "/orders/{uuid}",
    summary="Buscar pedido por UUID",
    description="Retorna os detalhes completos de um pedido específico pelo seu UUID, incluindo itens, cliente, vendedor, pagamento e envio."
)
async def get_order(
    uuid: str = Path(..., description="UUID único do pedido. Ex: ORD-2025-0001")
):
    with engine.connect() as conn:
        pedido = conn.execute(text("SELECT * FROM pedido WHERE uuid = :uuid"), {"uuid": uuid}).fetchone()
        if not pedido:
            return JSONResponse(status_code=404, content={"error": "Pedido não encontrado"})

        return _build_order_payload(conn, pedido)


@app.get(
    "/orders",
    summary="Listar pedidos",
    description="Retorna uma lista paginada de pedidos com suporte a filtros por cliente, produto e status, além de ordenação por campo e direção."
)
async def list_orders(
    codigoCliente: int | None = Query(default=None, description="Filtrar pelo ID do cliente. Ex: 7788"),
    idProduto: int | None = Query(default=None, alias="product_id", description="Filtrar pelo ID do produto. Ex: 9001"),
    status: str | None = Query(default=None, description="Filtrar por status do pedido. Valores aceitos: created, paid, separated, shipped, delivered, canceled"),
    page: int = Query(default=1, ge=1, description="Número da página (começa em 1)"),
    pageSize: int = Query(default=10, ge=1, le=100, description="Quantidade de registros por página (máximo 100)"),
    sortBy: str = Query(default="created_at", description="Campo para ordenação. Valores aceitos: created_at, total, status"),
    sortOrder: str = Query(default="desc", description="Direção da ordenação. Valores aceitos: asc, desc")
):
    try:
        validated_status = _validate_status(status)
        order_column, order_direction = _validate_sort(sortBy, sortOrder)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    offset = (page - 1) * pageSize

    with engine.connect() as conn:
        total_registros = conn.execute(text("""
            SELECT COUNT(DISTINCT p.uuid)
            FROM pedido p
            LEFT JOIN item_pedido i ON i.pedido_uuid = p.uuid
            WHERE (:codigo_cliente IS NULL OR p.cliente_id = :codigo_cliente)
              AND (:id_produto IS NULL OR i.product_id = :id_produto)
              AND (:status IS NULL OR p.status = :status)
        """), {
            "codigo_cliente": codigoCliente,
            "id_produto": idProduto,
            "status": validated_status
        }).scalar() or 0

        pedidos = conn.execute(text("""
            SELECT DISTINCT p.*
            FROM pedido p
            LEFT JOIN item_pedido i ON i.pedido_uuid = p.uuid
            WHERE (:codigo_cliente IS NULL OR p.cliente_id = :codigo_cliente)
              AND (:id_produto IS NULL OR i.product_id = :id_produto)
              AND (:status IS NULL OR p.status = :status)
            ORDER BY """ + order_column + " " + order_direction + """
            LIMIT :limit
            OFFSET :offset
        """), {
            "codigo_cliente": codigoCliente,
            "id_produto": idProduto,
            "status": validated_status,
            "limit": pageSize,
            "offset": offset
        }).fetchall()

        total_paginas = (int(total_registros) + pageSize - 1) // pageSize if total_registros else 0

        return {
            "orders": [_build_order_payload(conn, pedido) for pedido in pedidos],
            "pagination": {
                "page": page,
                "pageSize": pageSize,
                "totalRecords": int(total_registros),
                "totalPages": total_paginas,
                "sortBy": sortBy,
                "sortOrder": order_direction
            }
        }


if __name__ == "__main__":
    threading.Thread(target=start_consumer, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)