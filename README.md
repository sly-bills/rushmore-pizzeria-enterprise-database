# Rushmore Pizzeria Enterprise Database

An **Azure-based cloud-deployed PostgreSQL enterprise database system** for **RushMore Pizzeria**, designed and populated using **Python**, **Psycopg2** and **Faker** for **scalable, realistic data simulation and analytics**.

---

## Project Overview

This project demonstrates how to design, implement, and populate an enterprise-level **PostgreSQL database** hosted on **Microsoft Azure**. It serves as a data foundation for analytical tools such as **Tableau**, **Looker Studio**, and **Power BI**, enabling real-time business insights for a pizzeria franchise.

---

## Features

- Fully normalised **PostgreSQL schema** (3NF design)
- **Python-based data population** using 'Faker'
- **Data masking** for privacy and compliance
- **YAML-based configuration management**
- Cloud deployment via **Azure Database for PostgreSQL**
- **Scalable dataset generation** for analytics and reporting
- Seamless integration with **Tableau**

---

## Project Structure

| File / Folder | Description |
|---------------|-------------|
| 'pizzeria_schemaERD.pdf'       | ERD file showing the fully normalised PostgreSQL schema in 3rd Normal Form Design |
| 'pizzeria_schema.sql'          | SQL script to create database schema and tables                                   |
| 'dbconfig.yaml'                | Database connection configuration file                                            |
| 'populate.py'                  | Python script for data population                                                 |
| 'Analytics & Queries'          | Contains SQL scripts for business analysis                                        |
| 'Requirements.txt'             | Contains all foreign dependencies installed to run the python script              |
| '.gitignore'                   |                                                                                   |

---

### pizzeria_schemaERD.pdf - Database Entity Relationship Diagram

This file contains the Entity Realtionship Diagram (ERD) for the Rushmore Pizzeria Enterprise Database.
It visually outlines all tables such as Stores, Customers, Menu Items, Orders and their relationships forming the foundational blueprint used to design and implement the PostgreSQL schema. 

### dbconfig.yaml – Database Configuration File

This YAML file stores the secure connection settings for the RushMore Pizzeria PostgreSQL database. It contains details such as the host, port, username, password, and database name, allowing the Python script to connect to the Azure Database for PostgreSQL instance without hard-coding sensitive credentials directly in the code.

### pizzeria_schema.sql – PostgreSQL Database Schema

This script builds the complete structure of the database. It creates all tables along with their foreign keys, constraints and indexes. One of the issues I encountered here was foreign key dependency errors. Whenever i was deleting tables PostgreSQL raised errors because some tables depended on others through foreign keys. I solved it by dropping the tables in reverse dependency order. This ensured that no table was removed while still being referenced. I structured the DROP sequence at the start of the script. One other thing that also helped me was adding the BEGIN AND COMMIT queries at the start and end of the script respectively. This ran the whole script only if there were no errors. If there was an error it would stop running and traceback to the very beggining, till there are no errors. 

### populate.py - Python script for data population 

This is the script that i used to generate and populate the database for Rushmore Pizzeria Enterprise. 
I first of all had to create and activate a virtual environment that i would write the script from, then install the foreign dependencies that are not in-built into python and they were Faker, Psycopg2, PyYAML and tqdm. 
The faker is what was used to generate the fake PIIs
Psycopg2 was used to make a large number of INSERT's.
PyYAML was used to configure the yaml configurataion file.

These are some of the challenges i faced;

1. Some rows failed to insert because of missing foreign keys or dependency order issues. For example I inserted orders before customers existed. I solved it by structuring the population process in a strict logical process, the same as which my SQL script was created. This ensured all parent records existed before inserting child records.

2. Another problem i faced was duplicated and unrealistic faker data. Faker often generated repeated emails or unrealistic domain names, causing unique constraint violations. I solved it by creating a custom list of realistic email domains. Also to ensure masked uniqueness I added a short suffix to the local name of the email if a collision detected.

3.  When i ran the script i wasn't getting any progress feedback, making it hard to know if i was stuck. I therefore added **tqdm progress bars** to visually track each major data generation step, making the script more user friendly and easier to debug.


### Analysis Queries - PostgreSQL Business Insights
This file contains all the SQL query scripts for answering key business questions. They are in the following order;
1.	What is the total sales revenue per store?
2.	Who are the top 10 most valuable customers (by total spending)?
3.	What is the most popular menu item (by quantity sold) across all stores?
4.	What is the average order value?
5.	What are the busiest hours of the day for orders (e.g., 5 PM, 6 PM)?

### Requirements.txt

This file lists all the Python packages required for the RushMore Pizzeria project. It ensures that anyone running the system can easily install the exact libraries needed—such as Faker, psycopg2, tqdm and PyYAML making the environment fully reproducible and consistent across machines.

### .gitignore

This file specifies which files, logs, virtual environments, and credentials should be excluded from version control. It prevents sensitive information (like database configs), temporary files, and system-generated artifacts from being pushed to GitHub, keeping the repository clean and secure.


---


## How I Built and Ran the Project


1. Designed the Database Using an ERD

I began by designing a complete Entity Relationship Diagram (ERD) for the pizzeria database.
The ERD defined all key entities—Stores, Customers, Menu Items, Ingredients, Orders, and Order Items—and mapped out their relationships, constraints, and cardinality.
This became the blueprint for the SQL schema.

2. Deployed an Azure Database for PostgreSQL (Flexible Server)

Next, I created a fully managed Azure Database for PostgreSQL Flexible Server instance.
I configured all required settings, including:

Administrator username
Password
Server location
Firewall rules to allow access from my local machine

This set up the cloud environment where the database would live.

3. Connected the Azure Database to pgAdmin 4

Using pgAdmin 4, I connected to the Azure PostgreSQL server by:

Supplying the host name provided by Azure
Entering my admin credentials
Enabling SSL connection settings(Th)

Once connected, pgAdmin became my main tool for visually managing and monitoring the cloud database.

4. Created the Database Schema Using SQL

Using the ERD as a guide, I wrote and executed the pizzeria_schema.sql script.
In pgAdmin 4, I ran SQL queries to create the full database structure, including:

All tables
Primary and foreign keys
Constraints
Indexes

This transformed the ERD design into a fully functional relational database in Azure.

5. Connected VS Code to the Azure Database

To interact with the database programmatically, I connected my project in VS Code to the Azure database using:

dbconfig.yaml – a configuration file containing:

Host
Port
Username
Password
Database name

This allowed my Python scripts to establish secure and consistent connections to the cloud server.

6. Developed the populate.py Data Generation Script

I created populate.py to generate and insert realistic sample data using:

Faker for names, emails, addresses, phone numbers, etc.

psycopg2 for database connections

execute_values for efficient bulk inserts

tqdm for progress bars

Logging for debugging and transparency

The script:

Connects to Azure PostgreSQL

Generates stores, customers, ingredients, menu items

Builds realistic order and order‐item relationships

Inserts all records into the database while maintaining foreign key integrity

This automated data generation made the database analytics-ready.

7. Connected Tableau to the Azure PostgreSQL Database

Finally, I connected Tableau directly to the cloud database using PostgreSQL connectors.
With the populated tables, I was able to build dashboards and answer business analytical questions such as:

Order trends

Sales performance

Popular menu items

Ingredient usage patterns

Store-level comparisons

This completed the full end-to-end data engineering workflow—from design → cloud deployment → data generation → analytics.