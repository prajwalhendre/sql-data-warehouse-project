/* 

Script Purpose: 
  This script creates the database DataWarehouse. Also, it creates three schemas - "bronze", "silver", and "gold".

Warning:
  This script doesn't check if the database DataWarehouse already exists.


*/

use master;

-- to create the database

create database DataWarehouse

-- to create bronze, silver, and gold schemaas

create schema bronze;
go
create schema silver;
go
create schema gold;
go
