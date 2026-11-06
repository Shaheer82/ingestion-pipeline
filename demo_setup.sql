-- Isolated demo objects for the file-to-table matching milestone.
-- Run this in the PostgreSQL database you want to use for the demo.

-- CREATE SCHEMA IF NOT EXISTS ingest_demo;

-- CREATE TABLE IF NOT EXISTS ingest_demo.customers (
--     "Customer_ID" INTEGER,
--     full_name TEXT,
--     email TEXT
-- );

-- CREATE TABLE IF NOT EXISTS ingest_demo.orders (
--     order_id INTEGER,
--     customer_id INTEGER,
--     amount NUMERIC(10, 2)
-- );

-- -- These two intentionally share the same required column names so the
-- -- ambiguous.csv fixture should produce an ambiguous_match result.
-- CREATE TABLE IF NOT EXISTS ingest_demo.catalog_a (
--     code TEXT,
--     label TEXT
-- );

-- CREATE TABLE IF NOT EXISTS ingest_demo.catalog_b (
--     code TEXT,
--     label TEXT
-- );

-- Matching copies in public, as requested. They have the same column layouts
-- as the ingest_demo tables; keep SCHEMAS=ingest_demo for the original demo.
CREATE TABLE IF NOT EXISTS public.customers (
    customer_id INTEGER,
    full_name TEXT,
    email TEXT
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id INTEGER,
    customer_id INTEGER,
    amount NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS public.catalog_a (
    code TEXT,
    label TEXT
);

CREATE TABLE IF NOT EXISTS public.catalog_b (
    code TEXT,
    label TEXT
);

-- Run this in the PostgreSQL database used by the ingestion project.
-- The products.csv fixture has 10 fields; this target table has 8 columns.
CREATE TABLE IF NOT EXISTS public.products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    brand TEXT,
    unit_price NUMERIC(10, 2) NOT NULL,
    stock_quantity INTEGER NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_at DATE NOT NULL
);