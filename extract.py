import requests
import pandas as pd
import re
import pyodbc
from datetime import datetime
from pandas.tseries.offsets import DateOffset

# Configurações iniciais: token e ID do pipe
TOKEN_AUTH = "YOUR TOKEN"
PIPE_ID = "PIPE_ID"
API_URL = "https://api.pipefy.com/graphql"
HEADERS = {
    "Authorization": TOKEN_AUTH,
    "Content-Type": "application/json"
}

# Função para fazer a requisição paginada
def fetch_pipefy_cards(after_cursor=None):
    after_clause = f', after: "{after_cursor}"' if after_cursor else ""
    query = f"""
    {{
      cards(pipe_id: {PIPE_ID} {after_clause}) {{
        pageInfo {{
          endCursor
          hasNextPage
        }}
        edges {{
          node {{
            id
            title
            creatorEmail
            created_at
            finished_at
            updated_at
            due_date
            comments {{
              text
              created_at
            }}
            assignees {{
              id
              name
            }}
            labels {{
              id
              name
            }}
            created_by {{
              id
              name
            }}
            current_phase {{
              id
              name
            }}
            phases_history {{
              phase {{
                id
                name
                sequentialId
              }}
              firstTimeIn
              lastTimeIn
              lastTimeOut
              duration
            }}
            pipe {{
              id
              name
            }}
            fields {{
              name
              value
              array_value
            }}
          }}
        }}
      }}
    }}
    """

    response = requests.post(API_URL, headers=HEADERS, json={"query": query})
    response.raise_for_status()  # Verifica erros HTTP
    data = response.json()

    # Verifica se há erro na resposta GraphQL
    if "errors" in data:
        raise ValueError(f"Erro na API Pipefy: {data['errors']}")

    return data["data"]["cards"]

# Coleta dos dados de todas as páginas
all_cards = []
next_cursor = None

while True:
    page_data = fetch_pipefy_cards(next_cursor)
    for edge in page_data["edges"]:
        card_data = edge["node"]

        # Transforma os fields em colunas
        fields_dict = {}
        for field in card_data["fields"]:
            # Define nomes para campos de valor e array
            field_name_value = field["name"] + "_value"
            field_name_array_value = field["name"] + "_array_value"

            # Limpeza e adição de valor
            if field_name_value in [
"list_of_field_name_values"
            ]:
                # Limpa caracteres indesejados em valores
                fields_dict[field_name_value] = re.sub(r'[\[\]\"]', '', str(field.get("value", "")))

            # Limpeza e adição de array_value
            if field_name_array_value in [
"list_of_field_name_array_values"
            ]:
                # Limpa caracteres indesejados em arrays
                fields_dict[field_name_array_value] = re.sub(r"[\[\]']", '', str(field.get("array_value", "")))

        # Remove fields e atualiza o card com as colunas de fields
        card_data.pop("fields", None)
        card_data.update(fields_dict)

        # Adiciona o card processado à lista
        all_cards.append(card_data)

    # Verifica se há uma próxima página
    if page_data["pageInfo"]["hasNextPage"]:
        next_cursor = page_data["pageInfo"]["endCursor"]
    else:
        break

# Transformar os dados em DataFrame e manter apenas as colunas especificadas
df = pd.DataFrame(all_cards)

# Lista final de colunas desejadas
colunas_desejadas = [
"your_columns_here"
]

# Filtrar apenas as colunas desejadas e aplicar renomeações conforme especificado
df_filtered = df[colunas_desejadas].copy()

# Renomear colunas e limpar dados
df_filtered = df_filtered.rename(columns={
    "original_column": "renamed_column"
})

# Salvar o DataFrame filtrado e processado em Excel
df_filtered.to_excel('/home/usr/Pipefy/extract.xlsx', index=False)
print("Arquivo Excel gerado com sucesso: extract.xlsx")
