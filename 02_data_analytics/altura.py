# Definir a variável antes de usá-la
qtd_pessoas = int(input("Digite a quantidade de pessoas: "))

while qtd_pessoas != 0:
    altura = float(input("Digite a altura da pessoa: "))
    print(f"Altura registrada: {altura}m")

    # Atualizar qtd_pessoas para evitar loop infinito
    qtd_pessoas -= 1
