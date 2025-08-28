from pythonping import ping
import time

def verificar_oscilacao_na_rede(host):
    # Realizar pings em intervalos regulares
    while True:
        # Realizar um ping
        response_list = ping(host, count=5, timeout=1)

        # Coletar latências dos pings
        latencias = [response.time for response in response_list if response.success]

        # Verificar se houve sucesso nos pings
        if len(latencias) > 0:
            # Calcular média e desvio padrão das latências
            media = sum(latencias) / len(latencias)
            desvio_padrao = (sum((x - media) ** 2 for x in latencias) / len(latencias)) ** 0.5

            # Verificar se houve oscilação significativa
            if desvio_padrao > 10:  # Valor arbitrário para determinar oscilação significativa
                print(f"Oscilação significativa na rede! Média de latência: {media:.2f} ms, Desvio padrão: {desvio_padrao:.2f} ms")
            else:
                print(f"Latência estável. Média de latência: {media:.2f} ms, Desvio padrão: {desvio_padrao:.2f} ms")

        else:
            print("Falha ao realizar ping. Verifique a conexão com a rede.")

        # Aguardar 10 segundos antes do próximo ping
        time.sleep(10)

# Endereço IP ou hostname do servidor para ping
host = "endereco_do_servidor"

# Executar a função para verificar oscilação na rede do servidor
verificar_oscilacao_na_rede(host)
