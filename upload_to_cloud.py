import pyodbc
import pandas as pd
import numpy as np

# Conexão com o banco de dados Azure SQL
connection_string = 
"DRIVER={ODBC Driver 18 for SQL Server};SERVER=your_server;DATABASE=your_data_base;UID=your_usarid;PWD=yourpassword"
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Definir a criação da tabela com os tipos corretos
create_table_query = """
CREATE TABLE dbname.table_name (
your_columns
)
"""

# Executar a criação da tabela
try:
    cursor.execute("DROP TABLE IF EXISTS dbname.table_name")
    cursor.execute(create_table_query)
    conn.commit()
    print("Tabela criada com sucesso.")
except Exception as e:
    print(f"Erro ao criar tabela: {e}")

# Carregar o DataFrame e ajustar os tipos
df_churn = pd.read_excel("/home/usr/extract.xlsx")

# Inserir os dados na tabela criada
for index, row in df_churn.iterrows():
    cursor.execute("""
        INSERT INTO dbname.table_name (
your_columns
        ) VALUES (?)
    """,
    row['your_columns'],
    )

conn.commit()
print("Dados inseridos com sucesso.")
cursor.close()
conn.close()
