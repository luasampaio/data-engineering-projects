"""
Closure e funções que retornam outras funções

Em Python, um fechamento é normalmente uma função definida dentro de outra função.
Essa função interna captura os objetos definidos em seu escopo e os associa ao próprio objeto da função interna.

"""


def criar_saudacao(saudacao):
    def saudar(nome):
        return f'{saudacao}, {nome}!'
    return saudar


falar_bom_dia = criar_saudacao('Bom dia')
falar_boa_noite = criar_saudacao('Boa noite')
falar_boa_tarde = criar_saudacao('Boa tarde')


# for nome in ['Maria', 'Joana', 'Luciana']:
#     print(falar_bom_dia(nome))
#     print(falar_boa_noite(nome))

print(falar_bom_dia('Maria'))

print(falar_boa_noite('Ana'))

print(falar_boa_tarde('Joana'))

