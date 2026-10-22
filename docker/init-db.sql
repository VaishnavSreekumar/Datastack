-- Database Initialization Script for DataStack Warehouse
-- Executed automatically on container creation or manual setup

CREATE SCHEMA IF NOT EXISTS dev;
CREATE SCHEMA IF NOT EXISTS public;

GRANT ALL ON SCHEMA dev TO olist;
GRANT ALL ON SCHEMA public TO olist;

ALTER DEFAULT PRIVILEGES IN SCHEMA dev GRANT ALL ON TABLES TO olist;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO olist;
