-- pizzeria_schema.sql


-- 1. Create Schema and Set as Default
CREATE SCHEMA IF NOT EXISTS pizzeria;
SET search_path TO pizzeria;

-- 2. Drop tables in reverse dependency order to avoid foreign key errors
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS item_ingredients;
DROP TABLE IF EXISTS menu_items;
DROP TABLE IF EXISTS ingredients;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS stores;

-- 3. Creating Tables
-- Creating Stores Table
CREATE TABLE Stores (
	store_id SERIAL PRIMARY KEY,
	address VARCHAR(255) NOT NULL,
	city VARCHAR(100) NOT NULL,
	phone_number VARCHAR(20) UNIQUE NOT NULL,
	opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP	
);

-- creating Customers Table 
CREATE TABLE Customers (
	customer_id SERIAL PRIMARY KEY,
	first_name VARCHAR(100) NOT NULL,
	last_name VARCHAR(100) NOT NULL,
	email VARCHAR(255) UNIQUE NOT NULL,
	phone_number VARCHAR(20) UNIQUE NOT NULL,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Creating Ingredients Table 
CREATE TABLE Ingredients (
	ingredient_id SERIAL PRIMARY KEY,
	name VARCHAR(100) UNIQUE NOT NULL,
	stock_quantity NUMERIC(10, 2) NOT NULL DEFAULT 0,
	unit VARCHAR(20) NOT NULL
);

-- Creating Menu Items Table 
CREATE TABLE Menu_Items (
	item_id SERIAL PRIMARY KEY, 
	name VARCHAR(150) NOT NULL,
	category VARCHAR(50) NOT NULL,
	size VARCHAR(20),
	price NUMERIC(10, 2) NOT NULL
);

-- Creating Item_Ingredients Table 
CREATE TABLE Item_Ingredients (
	item_id INTEGER NOT NULL,
	ingredient_id INTEGER NOT NULL,
	quantity_required NUMERIC(10, 2) NOT NULL,
	PRIMARY KEY (item_id, ingredient_id),
	FOREIGN KEY (item_id) REFERENCES menu_items(item_id) ON DELETE CASCADE,
	FOREIGN KEY (ingredient_id) REFERENCES ingredients(ingredient_id) ON DELETE RESTRICT
);

-- Creating orders Table 
CREATE TABLE orders (
	order_id SERIAL PRIMARY KEY,
	customer_id INTEGER,
	store_id INTEGER NOT NULL,
	order_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	total_amount NUMERIC(10, 2) NOT NULL,
	status VARCHAR(50) NOT NULL DEFAULT 'Pending',
	FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE SET NULL,
	FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE RESTRICT
);

-- Creating Order_Items Table
CREATE TABLE order_items (
	order_item_id SERIAL PRIMARY KEY,
	order_id INTEGER NOT NULL,
	item_id INTEGER NOT NULL,
	quantity INTEGER NOT NULL CHECK (quantity > 0),
	price_at_time_of_order NUMERIC(10, 2) NOT NULL,
	FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
	FOREIGN KEY (item_id) REFERENCES menu_items(item_id) ON DELETE RESTRICT
);


-- Indexes
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_item_id ON order_items(item_id);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_store_id ON orders(store_id);
CREATE INDEX idx_item_ingredients_ingredient_id ON item_ingredients(ingredient_id);


 
