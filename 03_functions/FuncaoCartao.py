class ContaBancaria:
    def __init__(self, titular, saldo=0):
        self.titular = titular
        self.saldo = saldo

    def depositar(self, valor):
        self.saldo += valor


    def sacar(self, valor):
        if valor <= self.saldo:
            self.saldo -= valor
        else:
            print("Saldo insuficiente.")

    def mostrar_saldo(self):
        print(f"Titular: {self.titular} | Saldo: {self.saldo}")

# Usando a classe:
c = ContaBancaria("Luciana", 1000)
c.mostrar_saldo()  
c.depositar(500)
c.sacar(200)
c.mostrar_saldo()
c.sacar(1500)  # Tentativa de saque maior que o saldo    


