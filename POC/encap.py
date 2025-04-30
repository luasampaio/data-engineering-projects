# Encapsulamento no Python

class Heroi: 
    def __init__(self, nome, idade, poder):
        self.nome = nome
        self.idade = idade
        self.poder = poder

class Vilao:
    def __init__(self, nome, idade, poder):
        self.nome = nome
        self.idade = idade
        self.poder = poder

class Personagem:
    def __init__(self, nome, idade, poder, tipo):
        self.nome = nome
        self.idade = idade
        self.poder = poder
        self.tipo = tipo

class Habilidades:
    def __init__(self, nome, descricao):
        self.nome = nome
        self.descricao = descricao

# Criando instâncias corretamente
heroi = Heroi('Thor', 1500, 'Raio')
vilao = Vilao('Loki', 1500, 'Ilusão')
personagem = Personagem('Thor', 1500, 'Raio', 'heroi')
habilidade = Habilidades('Raio', 'Raio que sai do martelo')

# Acessando atributos corretamente
print(heroi.nome)  # Agora funciona corretamente
