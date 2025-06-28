class Pessoa:
    def __init__(self, nome, idade):
        self.nome = nome    # atributo
        self.idade = idade  # atributo

    def falar(self):        # método
        print(f"Olá, meu nome é {self.nome} e tenho {self.idade} anos.")




# Criar um objeto (instância)
p = Pessoa("Luciana", 30)

# Acessar atributos
print(p.nome)  # Luciana

# Chamar método
p.falar()  

p.nome = "João"  # Modificar atributo
print(p.nome)  # João


