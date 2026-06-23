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

def bq_to_supabase():
    print("🤖 [Fluxo 1] BigQuery -> Supabase...")
    
    query = """
        SELECT Id, Name, District_Name, City_Name, State, Available_Vehicles, Status, Parking_Lots, Parking_Lot_Price,
               Contac_Name, Contact_Telefone, Contact_Email, Start_Date, Overbooking, Latitude, Longitude, H3_Cell, Drive_Pictures,
               Observations, Operation_24h, Available, Vehicle_Entrance, Pedestrian_Entrance, Cover_Type, Floor_Type, Vacancies_Configuration, Fire_Protection, Wash,
               Designated_Parking_Space, Security_Camera, Guardhouse, Change_turn_24h, Blacklist, Ev_Charger, Router_Place, Amplifier_Place, Starkink_Place, Marketing_Options, Light_Indicator
        FROM `turbi-dc-ops.pods.tb_pods` 
    """
    query_job = client.query(query)
    
    lista_estacionamentos = []
    
    for row in query_job.result():
        data = {
            "id_friday": row.Id,               
            "nome_pod": row.Name,                   
            "status": row.Status,               
            "indicador_luminoso": row.Light_Indicator, 
            "distrito": row.District_Name,
            "cidade": row.City_Name,
            "estado": row.State,
            "vagas_disponiveis": row.Available_Vehicles,
            "capacidade_total": row.Parking_Lots,
            "parkinglotprice": row.Parking_Lot_Price,
            "contato_nome": row.Contac_Name, 
            "telefone": row.Contact_Telefone,
            "email": row.Contact_Email,
            "created_at": str(row.Start_Date) if row.Start_Date else None, 
            "check_overbooking": row.Overbooking, # Removido o str() para preservar booleano se aplicável
            "latitude": row.Latitude,
            "longitude": row.Longitude,
            "h3_cell_res_8": row.H3_Cell,
            "link_drive": row.Drive_Pictures,
            "observacoes": row.Observations,
            "operacao_24h": row.Operation_24h,
            "disponivel": row.Available,
            "entrada_veiculos": row.Vehicle_Entrance,
            "entrada_pedestres": row.Pedestrian_Entrance,
            "tipo_cobertura": row.Cover_Type,
            "pavimento": row.Floor_Type,
            "configuracao_vagas": row.Vacancies_Configuration,
            "protecao_incendio": row.Fire_Protection,
            "operacao_lavagem": row.Wash,
            "check_demarcado": row.Designated_Parking_Space,
            "check_cameras": row.Security_Camera,
            "check_guarita": row.Guardhouse,
            "check_virar_24h": row.Change_turn_24h,
            "is_blacklisted": row.Blacklist,
            "has_ev_charger": row.Ev_Charger,
            "local_roteador": row.Router_Place,
            "local_amplificador": row.Amplifier_Place,
            "local_starlink": row.Starkink_Place, 
            "enxoval_marketing": row.Marketing_Options
        }
        lista_estacionamentos.append(data)
    
    if lista_estacionamentos:
        print(f"Enviando {len(lista_estacionamentos)} registros para o Supabase...")
        supabase.table("pods").upsert(lista_estacionamentos).execute()
        
    print("✅ Fluxo BigQuery -> Supabase concluído.")

def supabase_to_bq():
    print("🤖 [Fluxo 2] Supabase -> BigQuery...")
    
    # CORRIGIDO: Removido espaços e corrigido 'indicador_luminoso'
    colunas_supabase = (
        "id_friday, nome_pod, status, indicador_luminoso, distrito, cidade, estado, vagas_disponiveis, capacidade_total, parkinglotprice,"
        "contato_nome, telefone, email, created_at, check_overbooking, latitude, longitude, h3_cell_res_8, link_drive, observacoes,"
        "operacao_24h, disponivel, entrada_veiculos, entrada_pedestres, tipo_cobertura, pavimento, configuracao_vagas, protecao_incendio,"
        "operacao_lavagem, check_demarcado, check_cameras, check_guarita, check_virar_24h, is_blacklisted, has_ev_charger, local_roteador,"
        "local_amplificador, local_starlink, enxoval_marketing"
    )
    
    response = supabase.table("pods").select(colunas_supabase).execute()
    records = response.data
    
    if not records:
        print("Nenhum dado encontrado no Supabase para atualizar.")
        return

    records_validos = [r for r in records if r.get('id_friday') is not None]
    
    if not records_validos:
        print("Nenhum registro com 'id_friday' válido para atualizar no BQ.")
        return

    print(f"Preparando carga de {len(records_validos)} registros para o BigQuery...")

    linhas_para_bq = []
    for r in records_validos:
        linha = {
            "Id": r['id_friday'],
            "Light_Indicator": r['indicador_luminoso'],
            "Status": r['status'],
            "Parking_Lots": r['capacidade_total'],
            "Parking_Space_Price": r['parkinglotprice'],
            "Contac_Name": r['contato_nome'],
            "Contact_Telefone": r['telefone'],
            "Contact_Email": r['email'],
            "Observations": r['observacoes'],
            "Name": r['nome_pod'],
            "District_Name": r['distrito'],
            "City_Name": r['cidade'],
            "State": r['estado'],
            "Available_Vehicles": r['vagas_disponiveis'],
            "Start_Date": r['created_at'],
            "Overbooking": r['check_overbooking'],
            "Latitude": r['latitude'],
            "Longitude": r['longitude'],
            "H3_Cell": r['h3_cell_res_8'],
            "Drive_Pictures": r['link_drive'],
            "Operation_24h": r['operacao_24h'],
            "Available": r['disponivel'],
            "Vehicle_Entrance": r['entrada_veiculos'],
            "Pedestrian_Entrance": r['entrada_pedestres'],
            "Cover_Type": r['tipo_cobertura'],
            "Floor_Type": r['pavimento'],
            "Vacancies_Configuration": r['configuracao_vagas'],
            "Fire_Protection": r['protecao_incendio'],
            "Wash": r['operacao_lavagem'],
            "Designated_Parking_Space": r['check_demarcado'],
            "Security_Camera": r['check_cameras'],
            "Guardhouse": r['check_guarita'],  
            "Change_turn_24h": r['check_virar_24h'],  
            "Blacklist": r['is_blacklisted'],
            "Ev_Charger": r['has_ev_charger'], 
            "Router_Place": r['local_roteador'],
            "Amplifier_Place": r['local_amplificador'],
            "Starkink_Place": r['local_starlink'], 
            "Marketing_Options": r['enxoval_marketing']  
        }
        linhas_para_bq.append(linha)

    # CORRIGIDO: dataset espera apenas o ID do dataset ("pods")
    dataset_ref = client.dataset("pods") 
    stage_table_ref = dataset_ref.table("tb_pods_stage")
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE 
    )
    
    print("Enviando dados para a tabela de estágio no BigQuery...")
    load_job = client.load_table_from_json(linhas_para_bq, stage_table_ref, job_config=job_config)
    load_job.result() 

    print("Executando o MERGE de atualização na tabela principal...")
    # CORRIGIDO: Adicionado fechamento de aspas triplas no final da string
    merge_query = """
        MERGE `turbi-dc-ops.pods.tb_pods` T
        USING `turbi-dc-ops.pods.tb_pods_stage` S
        ON T.Id = S.Id
        WHEN MATCHED THEN
          UPDATE SET 
            T.Light_Indicator = S.Light_Indicator,
            T.Status = S.Status,
            T.Parking_Lots = S.Parking_Lots,
            T.Parking_Space_Price = S.Parking_Space_Price,
            T.Contac_Name = S.Contac_Name,
            T.Contact_Telefone = S.Contact_Telefone,
            T.Contact_Email = S.Contact_Email,
            T.Observations = S.Observations,
            T.Name = S.Name,
            T.District_Name = S.District_Name,
            T.City_Name = S.City_Name,
            T.State = S.State,
            T.Available_Vehicles = S.Available_Vehicles,
            T.Start_Date = S.Start_Date,
            T.Overbooking = S.Overbooking,
            T.Latitude = S.Latitude,
            T.Longitude = S.Longitude,
            T.H3_Cell = S.H3_Cell,
            T.Drive_Pictures = S.Drive_Pictures,
            T.Operation_24h = S.Operation_24h,
            T.Available = S.Available,
            T.Vehicle_Entrance = S.Vehicle_Entrance,
            T.Pedestrian_Entrance = S.Pedestrian_Entrance,
            T.Cover_Type = S.Cover_Type,
            T.Floor_Type = S.Floor_Type,
            T.Vacancies_Configuration = S.Vacancies_Configuration,
            T.Fire_Protection = S.Fire_Protection,
            T.Wash = S.Wash,
            T.Designated_Parking_Space = S.Designated_Parking_Space,
            T.Security_Camera = S.Security_Camera,
            T.Guardhouse = S.Guardhouse,
            T.Change_turn_24h = S.Change_turn_24h,
            T.Blacklist = S.Blacklist,
            T.Ev_Charger = S.Ev_Charger,
            T.Router_Place = S.Router_Place,
            T.Amplifier_Place = S.Amplifier_Place,
            T.Starkink_Place = S.Starkink_Place,
            T.Marketing_Options = S.Marketing_Options
    """
    client.query(merge_query).result()
    
    print("✅ Fluxo Supabase -> BigQuery concluído com sucesso total utilizando MERGE!")

if __name__ == "__main__":
    bq_to_supabase()
    supabase_to_bq()
