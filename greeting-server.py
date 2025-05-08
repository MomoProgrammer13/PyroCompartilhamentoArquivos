import Pyro5.api
import random

@Pyro5.api.expose
class RioRacingServer(object):
    def __init__(self):
        self.frases_corrida = [
            "Não importa se você ganha por um segundo ou um quilômetro. Ganhar é ganhar.",
            "A estrada à frente é o que importa, não o que ficou para trás.",
            "No Rio, as regras são diferentes, mas o respeito continua sendo tudo.",
            "Não se trata de carros, trata-se de família.",
            "Quanto mais quente o lugar, mais legais são as pessoas."
        ]

    def frase_motivacional(self, nome):
        print(f"[Conexão recebida] Cliente '{nome}' solicitou uma frase motivacional")
        return f"Ei {nome}, aqui no Rio aprendemos que: {random.choice(self.frases_corrida)}"

    def plano_assalto(self, local):
        print(f"[Conexão recebida] Cliente solicitou plano de assalto para o local: '{local}'")
        return f"Assalto em {local}? Precisamos de precisão, uma equipe confiável e carros velozes!"

    def desafio_corrida(self, nome_piloto):
        print(f"[Conexão recebida] Cliente '{nome_piloto}' solicitou simulação de corrida")
        resultados = ["venceu na Avenida Atlântica!", "derrapou nas curvas de Santa Teresa!",
                      "impressionou a todos na descida da Vista Chinesa!"]
        return f"{nome_piloto} {random.choice(resultados)}"


daemon = Pyro5.server.Daemon()
ns = Pyro5.api.locate_ns()
uri = daemon.register(RioRacingServer)
ns.register("rio.racing", uri)

print("Servidor pronto. Aguardando conexões...")
print(f"URI registrada: {uri}")
daemon.requestLoop()