import Pyro5.api
import time


def exibir_menu():
    print("\n==== CORRIDA NO RIO ====")
    print("1. Receber frase motivacional")
    print("2. Planejar assalto")
    print("3. Simular uma corrida")
    print("4. Sair")
    return input("Escolha uma opção: ")


# Conectar ao servidor remoto
print("Conectando ao servidor remoto...")
try:
    rio_server = Pyro5.api.Proxy("PYRONAME:rio.racing")

    nome = input("Qual seu nome, piloto? ").strip()
    print(f"Bem-vindo ao Rio de Janeiro, {nome}!")

    opcao = ""
    while opcao != "4":
        opcao = exibir_menu()

        if opcao == "1":
            print("\n" + rio_server.frase_motivacional(nome))
        elif opcao == "2":
            local = input("Onde será o assalto? ")
            print("\n" + rio_server.plano_assalto(local))
        elif opcao == "3":
            print("\nSimulando corrida nas ruas do Rio...")
            time.sleep(1)
            print("3... 2... 1... VAI!")
            time.sleep(2)
            print(rio_server.desafio_corrida(nome))
        elif opcao == "4":
            print("Até a próxima missão!")
        else:
            print("Opção inválida, tente novamente.")

except Exception as e:
    print(f"Erro ao conectar: {e}")