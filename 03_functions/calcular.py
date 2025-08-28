def calculate_total(preço, quantidade):
    try:
        quantidade = int(quantidade)  # Converte quantidade para inteiro
        total = preço * quantidade
        print(f"Total é: {total}")
        return total
    except ValueError:
        print("Erro: quantidade deve ser um número.")
        return None

calculate_total(10, '5')  # Teste com string que pode ser convertida
calculate_total(10, '2')  # Teste com string inválida
calculate_total(10, 3)    # Teste com número inteiro