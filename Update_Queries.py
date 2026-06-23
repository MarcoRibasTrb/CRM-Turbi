from datetime import datetime
import pytz

def rodar_atualizacoes_bq():
    print("🤖 Iniciando atualizações agendadas no BigQuery...")
    
    # 1. A tabela de 2 em 2 horas roda SEMPRE
    print("Executando procedure de 2 horas...")
    client.query("CALL `turbi-dc-ops.pods.update_tb_ruptura`()").result()
    
    # 2. Checa o fuso horário de Brasília
    fuso_brasilia = pytz.timezone("America/Sao_Paulo")
    hora_atual = datetime.now(fuso_brasilia).hour
    
    # Se for a rodada das 06h da manhã, dispara os scripts pesados diários
    if hora_atual == 6:
        print("🌅 São 5h da manhã em Brasília. Disparando procedures diárias...")
        client.query("CALL `turbi-dc-ops.pods.update_tb_statusfrota`()").result()
        client.query("CALL `turbi-dc-ops.pods.update_tb_pods`()").result()
    else:
        print(f"Ignorando rotinas diárias (Hora atual: {hora_atual}h, roda apenas às 6h).")
        
    print("✅ Todas as rotinas do BigQuery foram processadas!")
