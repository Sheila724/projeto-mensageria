# Projeto Mensageria

## 🎯 Objetivo

Implementar um sistema de mensageria assíncrona usando **RabbitMQ** para processar pedidos de um marketplace, persistindo os dados em banco relacional PostgreSQL e expondo uma API RESTful para consulta.

## 🛠️ Tecnologias Utilizadas

- **RabbitMQ** (container) - Broker de mensagens
- **PostgreSQL** (container) - Banco relacional
- **FastAPI** (Python 3.11) - API REST
- **Docker + Docker Compose** - Containerização completa
- **SQLAlchemy** - ORM
- **pika** - Cliente RabbitMQ

## 📋 Como Executar o Projeto

```bash
docker compose up -d --build
```
## 2. Acessar os serviços

| Serviço              | URL                             |
| --------             | --------------------------------| 
| API (Swagger)        | http://localhost/docs  | 
| RabbitMQ Management  | http://localhost:15672      | 
| PostgreSQL           | localhost:5432                  |

## 3. Testar

- Enviar pedido de teste: **docker exec -it api-mensageria python producer.py**
- Consultar pedidos: http://localhost:8000/orders
- Consultar pedido específico: http://localhost/orders/ORD-2025-0001

## 4. Teste de carga e Monitoramento em Tempo Real

O grande diferencial deste projeto é a validação de resiliência. Para estressar a arquitetura e validar o comportamento do broker, o repositório inclui um script de disparo em lotes e um monitor dedicado.

 - Iniciar o monitor da fila (em um terminal separado): docker exec -it api-mensageria python monitor.py

- Executar o script de teste de estresse: docker exec -it api-mensageria python stress_test.py

Resultado: O sistema é capaz de processar envios de 10.000 mensagens por vez. Durante os testes, o RabbitMQ chegou a enfileirar mais de 184 mil requisições, mantendo a estabilidade da taxa de consumo e a resiliência do worker enquanto o monitor exibia o status da fila em tempo real.

## ✅ Entregas realizadas com este projeto

- [x] **Consumidor RabbitMQ:** processando mensagens ativamente.
- [x] **Persistência Relacional:** banco estruturado com tabelas `pedido`, `cliente`, `produto` e `item_pedido`.
- [x] **Registro Automático:** campo `indexed_at` gravado na inserção.
- [x] **API RESTful:** endpoints configurados com paginação, ordenação e filtros.
- [x] **Payload de Resposta:** retorno formatado estritamente conforme o modelo do PDF.
- [x] **Cálculo Dinâmico:** `total` do pedido e de cada item calculados via código.
- [x] **Docker Compose:** orquestração completa dos containers.
- [x] **Modelagem de Dados:** diagrama DER documentado.

---

## 📊 DER do Banco de Dados

![Diagrama Entidade Relacionamento](docs/DER.png)

*(Caso a imagem não carregue acima, [clique aqui para visualizar o DER](docs/DER.png))*

---

## 📁 Estrutura do Projeto

```text
projeto-mensageria/
├── app/
│   ├── main.py
│   ├── producer.py
│   ├── producer_auto.py
|   ├── monitor.queue.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── database/
│       └── init.sql
├── docker-compose.yml
├── README.md
└── docs/
    └── DER.png
