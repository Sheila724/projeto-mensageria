import pika
import time
import json
from datetime import datetime

RABBITMQ_HOST = "localhost"
RABBITMQ_USER = "admin"
RABBITMQ_PASS = "Gabi0510"
QUEUE_NAME = "orders"

credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)

print("Monitor da fila iniciado (atualiza a cada 2 segundos)")

try:
    while True:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Obtém informações da fila
        queue_info = channel.queue_declare(queue=QUEUE_NAME, passive=True)
        
        message_count = queue_info.method.message_count
        consumer_count = queue_info.method.consumer_count
        
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] "
              f"Fila: {QUEUE_NAME} | "
              f"Mensagens: {message_count} | "
              f"Consumers: {consumer_count}")
        
        if message_count > 0:
            print("   → A fila está com mensagens pendentes!")
        
        connection.close()
        time.sleep(2)   # atualiza a cada 2 segundos

except KeyboardInterrupt:
    print("\nMonitor encerrado.")
except Exception as e:
    print(f"Erro: {e}")

