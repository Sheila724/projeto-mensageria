-- DER: https://dbdiagram.io/d/projeto-mensageria-67f0a0c8b4a0e2a1c3d4e5f6 (link gerado depois)

CREATE TABLE cliente (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    documento VARCHAR(20)
);

CREATE TABLE produto (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    categoria VARCHAR(100)
);

CREATE TABLE pedido (
    uuid VARCHAR(50) PRIMARY KEY,
    created_at TIMESTAMP NOT NULL,
    indexed_at TIMESTAMP NOT NULL DEFAULT NOW(),   -- hora que indexamos
    channel VARCHAR(50),
    total DECIMAL(12,2),
    status VARCHAR(20),
    cliente_id INTEGER REFERENCES cliente(id),
    seller_id INTEGER,
    seller_nome VARCHAR(255),
    seller_cidade VARCHAR(100),
    seller_estado VARCHAR(2),
    shipment_carrier VARCHAR(100),
    shipment_service VARCHAR(100),
    shipment_status VARCHAR(50),
    shipment_tracking VARCHAR(100),
    payment_method VARCHAR(50),
    payment_status VARCHAR(50),
    payment_transaction_id VARCHAR(100),
    metadata JSONB
);

CREATE TABLE item_pedido (
    id SERIAL PRIMARY KEY,
    pedido_uuid VARCHAR(50) REFERENCES pedido(uuid),
    product_id INTEGER REFERENCES produto(id),
    product_name VARCHAR(255),
    unit_price DECIMAL(10,2),
    quantity INTEGER,
    total_item DECIMAL(12,2),
    categoria_id VARCHAR(10),
    categoria_nome VARCHAR(100),
    subcategoria_id VARCHAR(10),
    subcategoria_nome VARCHAR(100)
);
