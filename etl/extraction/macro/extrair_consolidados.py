import sys
import pymysql
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from config import db_destino  # noqa: E402

_DB_CONFIG = db_destino(autocommit=False)


def fetch_and_update_finalized_records(batch_size=2000):
    """
    Consulta `tabela_macros` com status='consolidado' e extraido=0 em lotes ordenados por id.
    Usa paginação por id-range (evita OFFSET) para garantir consistência sob escritas concorrentes.
    Marca cada lote como extraido=1 antes de avançar para o próximo.
    Retorna todos os registros extraídos como DataFrame.
    """
    connection = pymysql.connect(**_DB_CONFIG)

    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            last_id = 0
            all_records = []

            while True:
                # Paginação por id-range: evita OFFSET e é segura sob inserções concorrentes.
                # Filtra apenas registros consolidados ainda não extraídos.
                fetch_query = """
                    SELECT * FROM tabela_macros
                    WHERE status = 'consolidado'
                      AND extraido = 0
                      AND id > %s
                    ORDER BY id
                    LIMIT %s;
                """
                cursor.execute(fetch_query, (last_id, batch_size))
                records = cursor.fetchall()

                if not records:
                    break

                ids_to_update = [record['id'] for record in records]
                placeholders = ','.join(['%s'] * len(ids_to_update))
                update_query = f"""
                    UPDATE tabela_macros
                    SET extraido = 1,
                        data_extracao = NOW()
                    WHERE id IN ({placeholders});
                """
                cursor.execute(update_query, ids_to_update)
                connection.commit()

                print(f"Lote de {len(records)} registros marcado como extraído (ids {ids_to_update[0]}–{ids_to_update[-1]}).")

                all_records.extend(records)
                last_id = ids_to_update[-1]

            df = pd.DataFrame(all_records)
            return df

    except Exception as e:
        print(f"Erro ao consultar ou atualizar registros: {e}")
        return pd.DataFrame()

    finally:
        connection.close()

if __name__ == "__main__":
    extracted_data = fetch_and_update_finalized_records()
    if not extracted_data.empty:
        print("Registros extraídos e atualizados:")
        print(extracted_data)
    else:
        print("Nenhum registro novo para extrair.")