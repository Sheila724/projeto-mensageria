import pika
import json
import time
import random
from datetime import datetime, timezone
import uuid

# ================== CONFIGURAÇÃO ==================
RABBITMQ_HOST = "localhost"
RABBITMQ_USER = "admin"
RABBITMQ_PASS = "Gabi0510"
QUEUE_NAME = "orders"

BATCH_SIZE = 10000          # Quantidade de mensagens por rajada
PAUSE_SECONDS = 15       # Pausa após cada rajada (em segundos)
# =================================================

def generate_random_order():
    order_id = f"ORD-{datetime.now().year}-{random.randint(1000, 9999)}"
    
    customer_id = random.randint(1000, 9999)
    product_id = random.randint(9000, 9999)
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(99.90, 4999.99), 2)
    item_total = round(unit_price * quantity, 2)
    
    order = {
        "uuid": order_id,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "channel": random.choice(["mobile_app", "website", "marketplace", "app_store"]),
        "status": random.choice(["created", "paid", "separated", "shipped", "delivered", "canceled"]),
        "customer": {
            "id": customer_id,
            "name": random.choice(["Maria Oliveira", "João Silva", "Ana Costa", "Pedro Santos", "Laura Mendes"]),
            "email": f"cliente{random.randint(1,999)}@email.com",
            "document": f"{random.randint(100,999)}.{random.randint(100,999)}.{random.randint(100,999)}-{random.randint(10,99)}"
        },
        "seller": {
            "id": 55,
            "name": "Tech Store",
            "city": "São Paulo",
            "state": "SP"
        },
        "items": [{
            "id": random.randint(1, 100),
            "product_id": product_id,
            "product_name": random.choice(["Smartphone X", "Notebook Pro", "Fone Bluetooth", "Tablet Y", "Smartwatch Z"]),
            "unit_price": unit_price,
            "quantity": quantity,
            "category": {
                "id": "ELEC",
                "name": "Eletrônicos",
                "sub_category": {"id": "PHONE", "name": "Smartphones"}
            },
            "total": item_total
        }],
        "shipment": {
            "carrier": "Correios",
            "service": random.choice(["SEDEX", "PAC", "Express"]),
            "status": "shipped",
            "tracking_code": f"BR{random.randint(100000000,999999999)}"
        },
        "payment": {
            "method": random.choice(["pix", "credit_card", "boleto"]),
            "status": "approved",
            "transaction_id": f"pay_{uuid.uuid4().hex[:12]}"
        },
        "metadata": {
            "source": "test_script_rajada",
            "user_agent": "Mozilla/5.0 (Test Script)",
            "ip_address": f"192.168.{random.randint(1,255)}.{random.randint(1,255)}"
        }
    }
    return order

# ================== CONEXÃO ==================
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue=QUEUE_NAME, durable=True)

print(" Producer com RAJADAS iniciado!")
print(f"   → Enviando {BATCH_SIZE} mensagens por lote + pausa de {PAUSE_SECONDS}s\n")

try:
    batch_number = 1
    total_sent = 0

    while True:
        print(f" Enviando lote {batch_number} ({BATCH_SIZE} mensagens)...")
        
        for i in range(BATCH_SIZE):
            order = generate_random_order()
            message = json.dumps(order, ensure_ascii=False)
            
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            total_sent += 1

        print(f" Lote {batch_number} enviado! Total enviado até agora: {total_sent}")
        print(f" Pausando {PAUSE_SECONDS} segundos...\n")
        
        time.sleep(PAUSE_SECONDS)
        batch_number += 1

except KeyboardInterrupt:
    print(f"\n\n Producer parado. Total de pedidos enviados: {total_sent}")
finally:
    connection.close()