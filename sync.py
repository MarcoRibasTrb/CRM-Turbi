import os
from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from supabase import create_client, Client

# 1. Conexão com o Supabase
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)

# 2. Conexão com o BigQuery
creds = Credentials(
    token=None,
    refresh_token=os.environ.get("GCP_REFRESH_TOKEN"),
    token_uri="https://oauth2.googleapis.com/token",
    client_id=os.environ.get("GCP_CLIENT_ID"),
    client_secret=os.environ.get("GCP_CLIENT_SECRET")
)
client = bigquery.Client(credentials=creds, project="turbi-dc-ops")

def to_bool(value):
    """Converte valor para bool preservando None, independente de como a lib serializa."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == 'true'
    return bool(value)

def bq_to_supabase():
    print("🤖 [Fluxo 1] BigQuery -> Supabase...")
    
    # --- PASSO A: Baixar espelho do Supabase com paginação ---
    print("Baixando base atual do Supabase para cruzamento...")
    supabase_records = []
    limit = 1000
    offset = 0
    while True:
        res = supabase.table("pods").select("id_friday, nome_pod, status").range(offset, offset + limit - 1).execute()
        data = res.data
        supabase_records.extend(data)
        if len(data) < limit:
            break
        offset += limit
        
    # Criar dicionários de busca rápida
    supa_by_id = {str(r["id_friday"]): r for r in supabase_records if r.get("id_friday")}
    supa_by_nome = {r["nome_pod"].strip().lower(): r for r in supabase_records if r.get("nome_pod")}
    
    # --- PASSO B: Buscar dados do BigQuery ---
    # ATENÇÃO: Colunas renomeadas no BQ refletidas aqui
    query = """
        SELECT
            Id, Name, District_Name, City_Name, State, Available_Vehicles, Status,
            Parking_Lots, Parking_Lot_Price, Contac_Name, Contact_Telefone, Contact_Email,
            Start_Date, Overbooking, Latitude, Longitude, H3_Cell, Drive_Pictures,
            Observations,
            Is_Operation_24h,           -- era Operation_24h
            Is_Available,               -- era Available
            Vehicle_Entrance, Pedestrian_Entrance, Cover_Type, Floor_Type,
            Vacancies_Configuration, Fire_Protection, Wash,
            Is_Designated_Parking_Space, -- era Designated_Parking_Space
            Security_Camera,            -- agora STRING
            Has_Guardhouse,             -- era Guardhouse
            Change_turn_24h, Blacklist,
            Has_Ev_Charger,             -- era Ev_Charger
            Status_Ev_Charger,          -- NOVO campo
            Router_Place, Amplifier_Place, Starkink_Place, Marketing_Options,
            Has_Light_Indicator,        -- era Light_Indicator
            Light_Level,                -- NOVO campo (iluminação)
            Motivo_Encerramento,        -- NOVO
            Detalhe_Encerramento,       -- NOVO
            Data_Encerramento           -- NOVO
        FROM `turbi-dc-ops.pods.tb_pods`
    """
    query_job = client.query(query)
    
    lista_upsert = []
    
    print("Processando regras de cruzamento e atualização de dados...")
    for row in query_job.result():
        bq_id_str = str(row.Id)
        bq_name = row.Name
        bq_name_lower = bq_name.strip().lower() if bq_name else ""
        
        match = supa_by_id.get(bq_id_str)
        match_type = "id"
        
        if not match and bq_name_lower in supa_by_nome:
            match = supa_by_nome[bq_name_lower]
            match_type = "nome"

        data = {
            "id_friday":           row.Id,
            "nome_pod":            row.Name,
            "status":              row.Status,
            "indicador_luminoso":  row.Has_Light_Indicator,   # era Light_Indicator
            "distrito":            row.District_Name,
            "cidade":              row.City_Name,
            "estado":              row.State,
            "vagas_disponiveis":   row.Available_Vehicles,
            "capacidade_total":    row.Parking_Lots,
            "parkinglotprice":     row.Parking_Lot_Price,
            "contato_nome":        row.Contac_Name,
            "telefone":            row.Contact_Telefone,
            "email":               row.Contact_Email,
            "created_at":          str(row.Start_Date) if row.Start_Date else None,
            "check_overbooking":   bool(row.Overbooking) if row.Overbooking is not None else None,
            "latitude":            row.Latitude,
            "longitude":           row.Longitude,
            "h3_cell_res_8":       row.H3_Cell,
            "link_drive":          row.Drive_Pictures,
            "observacoes":         row.Observations,
            "operacao_24h":        row.Is_Operation_24h,       # era Operation_24h
            "disponivel":          row.Is_Available,           # era Available
            "entrada_veiculos":    row.Vehicle_Entrance,
            "entrada_pedestres":   row.Pedestrian_Entrance,
            "tipo_cobertura":      row.Cover_Type,
            "pavimento":           row.Floor_Type,
            "configuracao_vagas":  row.Vacancies_Configuration,
            "protecao_incendio":   row.Fire_Protection,
            "operacao_lavagem":    row.Wash,
            "check_demarcado":     row.Is_Designated_Parking_Space,  # era Designated_Parking_Space
            "check_cameras":       row.Security_Camera,        # agora string
            "check_guarita":       row.Has_Guardhouse,         # era Guardhouse
            "check_virar_24h":     row.Change_turn_24h,
            "is_blacklisted":      to_bool(row.Blacklist),
            "has_ev_charger":      row.Has_Ev_Charger,         # era Ev_Charger
            "status_ev_charger":   row.Status_Ev_Charger,      # NOVO
            "local_roteador":      row.Router_Place,
            "local_amplificador":  row.Amplifier_Place,
            "local_starlink":      row.Starkink_Place,
            "enxoval_marketing":   row.Marketing_Options,
            "nivel_iluminacao":    row.Light_Level,            # NOVO
            "motivo_encerramento": row.Motivo_Encerramento,    # NOVO
            "detalhe_encerramento": row.Detalhe_Encerramento,  # NOVO
            "data_encerramento":   str(row.Data_Encerramento) if row.Data_Encerramento else None,  # NOVO
            "origem_registro":     "base interna"
        }
        
        if match and match_type == "nome" and not match.get("id_friday"):
            supabase.table("pods").update(data).eq("nome_pod", match["nome_pod"]).execute()
        else:
            lista_upsert.append(data)
    
    if lista_upsert:
        print(f"Enviando {len(lista_upsert)} registros via UPSERT para o Supabase...")
        supabase.table("pods").upsert(
            lista_upsert,
            on_conflict="id_friday"
        ).execute()
        
    print("✅ Fluxo BigQuery -> Supabase concluído.")


def supabase_to_bq():
    print("🤖 [Fluxo 2] Supabase -> BigQuery...")
    
    colunas_supabase = (
        "id_friday, nome_pod, status, indicador_luminoso, distrito, cidade, estado, vagas_disponiveis, capacidade_total, parkinglotprice,"
        "contato_nome, telefone, email, created_at, check_overbooking, latitude, longitude, h3_cell_res_8, link_drive, observacoes,"
        "operacao_24h, disponivel, entrada_veiculos, entrada_pedestres, tipo_cobertura, pavimento, configuracao_vagas, protecao_incendio,"
        "operacao_lavagem, check_demarcado, check_cameras, check_guarita, check_virar_24h, is_blacklisted, has_ev_charger, status_ev_charger,"
        "local_roteador, local_amplificador, local_starlink, enxoval_marketing, nivel_iluminacao,"
        "motivo_encerramento, detalhe_encerramento, data_encerramento"
    )
    
    response = supabase.table("pods").select(colunas_supabase).not_.is_("id_friday", "null").execute()
    records_validos = response.data
    
    if not records_validos:
        print("Nenhum registro com 'id_friday' válido para atualizar no BQ.")
        return

    print(f"Preparando carga de {len(records_validos)} registros para o BigQuery...")

    linhas_para_bq = []
    for r in records_validos:
        linha = {
            "Id":                        r["id_friday"],
            "Has_Light_Indicator":       to_bool(r["indicador_luminoso"]),  # era Light_Indicator
            "Status":                    r["status"],
            "Parking_Lots":              r["capacidade_total"],
            "Parking_Lot_Price":         r["parkinglotprice"],
            "Contac_Name":               r["contato_nome"],
            "Contact_Telefone":          r["telefone"],
            "Contact_Email":             r["email"],
            "Observations":              r["observacoes"],
            "Name":                      r["nome_pod"],
            "District_Name":             r["distrito"],
            "City_Name":                 r["cidade"],
            "State":                     r["estado"],
            "Available_Vehicles":        r["vagas_disponiveis"],
            "Start_Date":                r["created_at"][:10] if r["created_at"] else None,
            "Overbooking":               int(r["check_overbooking"]) if r["check_overbooking"] is not None else None,
            "Latitude":                  r["latitude"],
            "Longitude":                 r["longitude"],
            "H3_Cell":                   r["h3_cell_res_8"],
            "Drive_Pictures":            r["link_drive"],
            "Is_Operation_24h":          to_bool(r["operacao_24h"]),         # era Operation_24h
            "Is_Available":              to_bool(r["disponivel"]),            # era Available
            "Vehicle_Entrance":          r["entrada_veiculos"],
            "Pedestrian_Entrance":       r["entrada_pedestres"],
            "Cover_Type":                r["tipo_cobertura"],
            "Floor_Type":                r["pavimento"],
            "Vacancies_Configuration":   r["configuracao_vagas"],
            "Fire_Protection":           r["protecao_incendio"],
            "Wash":                      r["operacao_lavagem"],
            "Is_Designated_Parking_Space": to_bool(r["check_demarcado"]),    # era Designated_Parking_Space
            "Security_Camera":           r["check_cameras"],                 # agora string
            "Has_Guardhouse":            to_bool(r["check_guarita"]),        # era Guardhouse
            "Change_turn_24h":           to_bool(r["check_virar_24h"]),
            "Blacklist":                 to_bool(r["is_blacklisted"]),
            "Has_Ev_Charger":            to_bool(r["has_ev_charger"]),       # era Ev_Charger
            "Status_Ev_Charger":         r["status_ev_charger"],             # NOVO
            "Router_Place":              r["local_roteador"],
            "Amplifier_Place":           r["local_amplificador"],
            "Starkink_Place":            r["local_starlink"],
            "Marketing_Options":         r["enxoval_marketing"],
            "Light_Level":               r["nivel_iluminacao"],              # NOVO
            "Motivo_Encerramento":        r["motivo_encerramento"],           # NOVO
            "Detalhe_Encerramento":       r["detalhe_encerramento"],          # NOVO
            "Data_Encerramento":          r["data_encerramento"],             # NOVO
        }
        linhas_para_bq.append(linha)

    dataset_ref = client.dataset("pods")
    stage_table_ref = dataset_ref.table("tb_pods_stage")
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("Id",                          "INTEGER"),
            bigquery.SchemaField("Name",                        "STRING"),
            bigquery.SchemaField("District_Name",               "STRING"),
            bigquery.SchemaField("City_Name",                   "STRING"),
            bigquery.SchemaField("State",                       "STRING"),
            bigquery.SchemaField("Available_Vehicles",          "INTEGER"),
            bigquery.SchemaField("Status",                      "STRING"),
            bigquery.SchemaField("Parking_Lots",                "INTEGER"),
            bigquery.SchemaField("Parking_Lot_Price",           "FLOAT"),
            bigquery.SchemaField("Contac_Name",                 "STRING"),
            bigquery.SchemaField("Contact_Telefone",            "STRING"),
            bigquery.SchemaField("Contact_Email",               "STRING"),
            bigquery.SchemaField("Start_Date",                  "DATE"),
            bigquery.SchemaField("Overbooking",                 "INTEGER"),
            bigquery.SchemaField("Latitude",                    "FLOAT"),
            bigquery.SchemaField("Longitude",                   "FLOAT"),
            bigquery.SchemaField("H3_Cell",                     "STRING"),
            bigquery.SchemaField("Drive_Pictures",              "STRING"),
            bigquery.SchemaField("Observations",                "STRING"),
            bigquery.SchemaField("Is_Operation_24h",            "BOOL"),     # renomeado
            bigquery.SchemaField("Is_Available",                "BOOL"),     # renomeado
            bigquery.SchemaField("Vehicle_Entrance",            "STRING"),
            bigquery.SchemaField("Pedestrian_Entrance",         "STRING"),
            bigquery.SchemaField("Cover_Type",                  "STRING"),
            bigquery.SchemaField("Floor_Type",                  "STRING"),
            bigquery.SchemaField("Vacancies_Configuration",     "STRING"),
            bigquery.SchemaField("Fire_Protection",             "STRING"),
            bigquery.SchemaField("Wash",                        "STRING"),
            bigquery.SchemaField("Is_Designated_Parking_Space", "BOOL"),    # renomeado
            bigquery.SchemaField("Security_Camera",             "STRING"),  # tipo alterado
            bigquery.SchemaField("Has_Guardhouse",              "BOOL"),    # renomeado
            bigquery.SchemaField("Change_turn_24h",             "BOOL"),
            bigquery.SchemaField("Blacklist",                   "BOOL"),
            bigquery.SchemaField("Has_Ev_Charger",              "BOOL"),    # renomeado
            bigquery.SchemaField("Status_Ev_Charger",           "STRING"),  # NOVO
            bigquery.SchemaField("Router_Place",                "STRING"),
            bigquery.SchemaField("Amplifier_Place",             "STRING"),
            bigquery.SchemaField("Starkink_Place",              "STRING"),
            bigquery.SchemaField("Marketing_Options",           "STRING"),
            bigquery.SchemaField("Has_Light_Indicator",         "BOOL"),    # renomeado
            bigquery.SchemaField("Light_Level",                 "STRING"),  # NOVO
            bigquery.SchemaField("Motivo_Encerramento",         "STRING"),  # NOVO
            bigquery.SchemaField("Detalhe_Encerramento",        "STRING"),  # NOVO
            bigquery.SchemaField("Data_Encerramento",           "TIMESTAMP"), # NOVO
        ]
    )
    
    print("Enviando dados para a tabela de estágio no BigQuery...")
    load_job = client.load_table_from_json(linhas_para_bq, stage_table_ref, job_config=job_config)
    load_job.result()

    print("Executando o MERGE de atualização restrita na tabela principal...")
    merge_query = """
        MERGE `turbi-dc-ops.pods.tb_pods` T
        USING `turbi-dc-ops.pods.tb_pods_stage` S
        ON T.Id = S.Id
        WHEN MATCHED THEN
          UPDATE SET
            T.Has_Light_Indicator        = S.Has_Light_Indicator,
            T.Observations               = S.Observations,
            T.Drive_Pictures             = S.Drive_Pictures,
            T.Is_Operation_24h           = S.Is_Operation_24h,
            T.Vehicle_Entrance           = S.Vehicle_Entrance,
            T.Pedestrian_Entrance        = S.Pedestrian_Entrance,
            T.Cover_Type                 = S.Cover_Type,
            T.Floor_Type                 = S.Floor_Type,
            T.Vacancies_Configuration    = S.Vacancies_Configuration,
            T.Fire_Protection            = S.Fire_Protection,
            T.Wash                       = S.Wash,
            T.Is_Designated_Parking_Space = S.Is_Designated_Parking_Space,
            T.Security_Camera            = S.Security_Camera,
            T.Has_Guardhouse             = S.Has_Guardhouse,
            T.Change_turn_24h            = S.Change_turn_24h,
            T.Has_Ev_Charger             = S.Has_Ev_Charger,
            T.Status_Ev_Charger          = S.Status_Ev_Charger,
            T.Router_Place               = S.Router_Place,
            T.Amplifier_Place            = S.Amplifier_Place,
            T.Starkink_Place             = S.Starkink_Place,
            T.Marketing_Options          = S.Marketing_Options,
            T.Light_Level                = S.Light_Level,
            T.Motivo_Encerramento        = S.Motivo_Encerramento,
            T.Detalhe_Encerramento       = S.Detalhe_Encerramento,
            T.Data_Encerramento          = S.Data_Encerramento
    """
    client.query(merge_query).result()
    
    print("✅ Fluxo Supabase -> BigQuery concluído com segurança total!")

if __name__ == "__main__":
    bq_to_supabase()
    supabase_to_bq()
