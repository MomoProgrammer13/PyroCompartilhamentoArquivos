import Pyro5.api
import Pyro5.errors
import threading
import time
import random
import os
import sys
import logging
from constants import (
    NAMESERVER_HOST, NAMESERVER_PORT, PEER_NAME_PREFIX, TRACKER_BASE_NAME,
    HEARTBEAT_INTERVAL, TRACKER_DETECTION_TIMEOUT_MIN, TRACKER_DETECTION_TIMEOUT_MAX,
    QUORUM, MAX_EPOCH_SEARCH, DOWNLOAD_CHUNK_SIZE, ELECTION_REQUEST_TIMEOUT
)

# configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(peer_id)s - %(message)s')


@Pyro5.api.expose
@Pyro5.api.behavior(instance_mode="single")
class Peer:
    def __init__(self, peer_id, shared_folder_path):
        self.peer_id = peer_id

        # configuração do logging para o arquivo deste peer
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)  # cria o diretório de logs caso não exista
        self.log_file_path = os.path.join(log_dir, f"{self.peer_id}_app.log")

        # remove handlers antigos do logger raiz para evitar logs duplicados
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(peer_id)s - %(message)s',
            filename=self.log_file_path,
            filemode='w'  # sobrescreve o log a cada execução
        )

        self.logger = logging.LoggerAdapter(logging.getLogger(__name__), {'peer_id': self.peer_id})

        self.uri = None
        self.pyro_daemon = None

        self.shared_folder = os.path.abspath(shared_folder_path)
        os.makedirs(self.shared_folder, exist_ok=True)
        self.local_files = self._scan_local_files()

        self.is_tracker = False
        self.current_tracker_uri_str = None
        self.current_tracker_proxy = None
        self.current_tracker_epoch = 0

        # atributos de eleição
        self.candidate_for_epoch = 0  # sinaliza se o peer está ativamente buscando votos (0 = não candidato)
        self.candidate_for_epoch_value = 0  # guarda a época para a qual a candidatura foi iniciada
        self.votes_received_for_epoch = {}
        self.voted_in_epoch = {}

        # timers
        self.tracker_timeout_timer = None
        self.heartbeat_send_timer = None
        self.election_vote_collection_timer = None

        self.logger.info(f"Peer inicializado. Pasta de compartilhamento: {self.shared_folder}")
        self.logger.info(f"Arquivos locais iniciais: {self.local_files}")

    def _setup_pyro(self):
        # configura o daemon Pyro e faz o registro no servidor de nomes
        try:
            self.pyro_daemon = Pyro5.server.Daemon(host=self._get_local_ip())
            self.uri = self.pyro_daemon.register(self)
            # obter um proxy NS local para registro inicial
            ns_proxy_setup = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
            ns_proxy_setup.register(f"{PEER_NAME_PREFIX}{self.peer_id}", self.uri)
            self.logger.info(f"Registrado no daemon PyRO com URI: {self.uri}")
            self.logger.info(f"Registrado no servidor de nomes como: {PEER_NAME_PREFIX}{self.peer_id}")
        except Pyro5.errors.NamingError:
            self.logger.error(
                f"Falha ao conectar/registrar no servidor de nomes em {NAMESERVER_HOST}:{NAMESERVER_PORT}. Verifique se está rodando.")
            sys.exit(1)
        except Exception as e:
            self.logger.error(f"Erro ao configurar PyRO: {e}")
            sys.exit(1)

    def _get_local_ip(self):
        # tenta descobrir o ip local para rodar o daemon
        return "localhost"

    def _scan_local_files(self):
        # varre a pasta compartilhada e retorna lista de arquivos locais
        try:
            return [f for f in os.listdir(self.shared_folder) if os.path.isfile(os.path.join(self.shared_folder, f))]
        except Exception as e:
            self.logger.error(f"Erro ao escanear arquivos locais: {e}")
            return []

    def update_local_files_and_notify_tracker(self):
        # atualiza lista de arquivos locais e avisa o tracker sobre mudanças
        old_files = set(self.local_files)
        self.local_files = self._scan_local_files()
        new_files = set(self.local_files)

        if old_files != new_files:
            self.logger.info(f"Mudança nos arquivos locais detectada. Novos arquivos: {self.local_files}")
            if self.current_tracker_uri_str and not self.is_tracker:  # modificado para usar URI
                try:
                    # criar proxy localmente para esta operação, especialmente se chamado de um thread diferente
                    tracker_proxy_local = Pyro5.api.Proxy(self.current_tracker_uri_str)
                    tracker_proxy_local._pyroTimeout = 5  # definir timeout
                    self.logger.info(
                        f"Notificando tracker {self.current_tracker_uri_str} (Época {self.current_tracker_epoch}) sobre mudança nos arquivos.")
                    # a resposta do register_files pode indicar se a época está baixa
                    response = tracker_proxy_local.register_files(self.peer_id, str(self.uri), self.local_files,
                                                                  self.current_tracker_epoch)
                    if isinstance(response, dict) and response.get("status") == "epoch_too_low":
                        self.logger.warning(
                            f"Tracker informou que minha época ({self.current_tracker_epoch}) é muito baixa. Tracker atual é época {response.get('current_tracker_epoch')}. Tentando reconectar/descobrir.")
                        self._discover_tracker()  # força nova descoberta
                except Pyro5.errors.CommunicationError:
                    self.logger.warning(
                        "Falha ao notificar tracker sobre mudança nos arquivos (CommunicationError). Tracker pode estar offline.")
                    self._handle_tracker_communication_error()
                except Exception as e:
                    self.logger.error(f"Erro ao notificar tracker: {e}")
            elif self.is_tracker:
                self.logger.info("Atualizando índice do tracker para meus próprios arquivos.")
                self._update_tracker_index_for_peer(self.peer_id, str(self.uri), self.local_files)

    def _discover_tracker(self):
        # busca um tracker ativo no servidor de nomes e conecta a ele
        self.logger.info("Procurando por um tracker ativo...")
        latest_epoch_found = -1
        tracker_uri_to_connect = None
        ns_proxy_local_discover = None

        try:
            ns_proxy_local_discover = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
        except Pyro5.errors.NamingError:
            self.logger.error(f"Servidor de nomes não encontrado em _discover_tracker.")
            # a lógica de eleição ou Peer1 se tornando tracker será acionada abaixo
            pass  # permite que o código abaixo lide com ns_proxy_local_discover sendo None

        if ns_proxy_local_discover:
            # procura do tracker com a maior época válida
            for i in range(MAX_EPOCH_SEARCH, -1, -1):  # começa da época mais alta possível
                tracker_name_to_find = f"{TRACKER_BASE_NAME}{i}"
                uri_obj_from_ns = None
                try:
                    uri_obj_from_ns = ns_proxy_local_discover.lookup(tracker_name_to_find)
                    if uri_obj_from_ns:
                        # criar um novo proxy para o tracker candidato no thread atual, usando str(uri_obj_from_ns)
                        temp_proxy = Pyro5.api.Proxy(str(uri_obj_from_ns))
                        temp_proxy._pyroTimeout = 1.5  # timeout curto para ping
                        temp_proxy.ping()

                        latest_epoch_found = i
                        tracker_uri_to_connect = uri_obj_from_ns  # armazena o objeto URI original
                        self.logger.info(f"Tracker encontrado: {tracker_name_to_find} com URI {uri_obj_from_ns} (Época {i}).")
                        break  # encontrou o mais recente, para a busca
                except Pyro5.errors.NamingError:
                    continue  # tracker para esta época não encontrado no NS
                except Pyro5.errors.CommunicationError:
                    self.logger.warning(
                        f"Tracker {tracker_name_to_find} (URI {uri_obj_from_ns if uri_obj_from_ns else 'desconhecido'}) encontrado no NS, mas não respondeu ao ping. Considerando-o falho.")
                    continue
                except Exception as e:
                    self.logger.warning(f"Erro ao tentar conectar/pingar ao tracker {tracker_name_to_find} (URI {uri_obj_from_ns if uri_obj_from_ns else 'desconhecido'}): {e}")
                    continue
        else:  # ns_proxy_local_discover is None
            self.logger.warning("Não foi possível conectar ao servidor de nomes durante a descoberta de tracker.")


        if tracker_uri_to_connect:
            self._connect_to_tracker(tracker_uri_to_connect, latest_epoch_found)
        else:
            # nenhum tracker ativo encontrado após varredura
            if self.peer_id == "Peer1" and self.current_tracker_epoch == 0 and latest_epoch_found == -1:
                initial_epoch_for_peer1 = 1
                self.logger.info(
                    f"Nenhum tracker ativo encontrado. Como sou {self.peer_id} e não conheço nenhuma época, tentarei me tornar o tracker inicial da Época {initial_epoch_for_peer1}.")

                ns_proxy_check_peer1 = None
                can_become_tracker = True
                try:
                    ns_proxy_check_peer1 = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
                    ns_proxy_check_peer1.lookup(f"{TRACKER_BASE_NAME}{initial_epoch_for_peer1}")
                    self.logger.info(
                        f"Um tracker para a Época {initial_epoch_for_peer1} já foi registrado por outro peer enquanto eu verificava. Iniciando eleição normal.")
                    can_become_tracker = False  # outro peer foi mais rápido
                except Pyro5.errors.NamingError:
                    # a Época 1 não está registrada, Peer1 pode assumir (se ns_proxy_check_peer1 foi obtido)
                    if not ns_proxy_check_peer1:  # falha ao conectar ao NS
                        self.logger.warning(f"Servidor de nomes inacessível ao tentar verificar {TRACKER_BASE_NAME}{initial_epoch_for_peer1} para Peer1. N��o posso me tornar tracker.")
                        can_become_tracker = False
                except Exception as e_check:
                    self.logger.error(f"Erro ao verificar tracker para Peer1: {e_check}")
                    can_become_tracker = False  # erro, não assume

                if can_become_tracker:
                    self.logger.info(f"Assumindo como Tracker inicial da Época {initial_epoch_for_peer1}.")
                    self._become_tracker(initial_epoch_for_peer1)
                else:
                    self.initiate_election()  # se não pode se tornar tracker, inicia eleição
            else:
                self.logger.info("Nenhum tracker ativo encontrado. Iniciando eleição.")
                self.initiate_election()

    def _connect_to_tracker(self, tracker_uri, epoch):
        # conecta ao tracker encontrado e registra arquivos
        tracker_uri_str = str(tracker_uri)  # garantir que é string para comparações e proxy
        try:
            # se já estou conectado a este tracker e época, não faz nada
            if self.current_tracker_uri_str == tracker_uri_str and self.current_tracker_epoch == epoch:
                self.logger.debug(f"Já conectado ao tracker {tracker_uri_str} da época {epoch}.")
                if not self.is_tracker: self._start_tracker_timeout_detection()  # garante que o timer está rodando
                return

            self.logger.info(f"Tentando conectar ao Tracker {tracker_uri_str} (Época {epoch}).")
            new_tracker_proxy = Pyro5.api.Proxy(tracker_uri_str)  # usar string URI
            new_tracker_proxy._pyroTimeout = 5

            # ping para confirmar que o novo tracker está realmente acessível antes de mudar
            new_tracker_proxy.ping()

            self.current_tracker_uri_str = tracker_uri_str
            self.current_tracker_proxy = new_tracker_proxy
            self.current_tracker_epoch = epoch
            self.is_tracker = (str(self.uri) == self.current_tracker_uri_str)

            if not self.is_tracker:
                self.logger.info(
                    f"Conectado ao Tracker_Epoca_{epoch} ({self.current_tracker_uri_str}). Registrando meus arquivos...")

                # cancela qualquer candidatura pendente se a época do tracker conectado for >= minha época de candidatura
                if hasattr(self, 'candidate_for_epoch_value') and \
                        self.candidate_for_epoch_value > 0 and \
                        self.candidate_for_epoch_value <= epoch:
                    self.logger.info(
                        f"Conectei ao tracker da época {epoch}. Cancelando minha candidatura pendente para época {self.candidate_for_epoch_value}.")
                    if self.election_vote_collection_timer and self.election_vote_collection_timer.is_alive():
                        self.election_vote_collection_timer.cancel()
                    self.candidate_for_epoch = 0  # não sou mais candidato ativo
                    self.votes_received_for_epoch.pop(self.candidate_for_epoch_value, None)
                    self.candidate_for_epoch_value = 0

                response = self.current_tracker_proxy.register_files(self.peer_id, str(self.uri), self.local_files,
                                                                     self.current_tracker_epoch)
                if isinstance(response, dict) and response.get("status") == "epoch_too_low":
                    self.logger.warning(
                        f"Ao registrar, tracker {self.current_tracker_uri_str} informou que minha época ({self.current_tracker_epoch}) é baixa. Tracker real é {response.get('current_tracker_epoch')}. Descobrindo novamente.")
                    self._clear_current_tracker()  # limpa o tracker incorreto
                    self.current_tracker_epoch = response.get('current_tracker_epoch',
                                                              self.current_tracker_epoch)  # atualiza para tentar a época correta
                    self._discover_tracker()
                    return  # evita iniciar timeout para tracker errado

                self._start_tracker_timeout_detection()
            else:  # eu sou o tracker
                self.logger.info(f"Eu sou o Tracker_Epoca_{epoch}.")
                self._stop_tracker_timeout_detection()  # tracker não detecta a si mesmo
                self._start_sending_heartbeats()  # garante que está enviando heartbeats

        except Pyro5.errors.CommunicationError:
            self.logger.error(f"Falha de comunicação ao conectar/registrar com Tracker_Epoca_{epoch} ({tracker_uri}).")
            self._clear_current_tracker()
            self.initiate_election()
        except Exception as e:
            self.logger.error(f"Erro ao conectar ao tracker {tracker_uri}: {e}")
            self._clear_current_tracker()
            self.initiate_election()

    def _clear_current_tracker(self):
        # limpa dados do tracker atual e cancela timers relacionados
        # self.logger.info(f"Limpando informações do tracker atual (URI: {self.current_tracker_uri_str}, Época: {self.current_tracker_epoch}).")
        self.current_tracker_uri_str = None
        self.current_tracker_proxy = None
        if self.is_tracker:  # se eu era o tracker
            self._stop_sending_heartbeats()
        self.is_tracker = False
        self._stop_tracker_timeout_detection()

    def _handle_tracker_communication_error(self):
        # trata falhas de comunicação com o tracker e inicia eleição
        tracker_uri_at_error = self.current_tracker_uri_str
        tracker_epoch_at_error = self.current_tracker_epoch
        self.logger.warning(
            f"Erro de comunicação com o tracker (URI: {tracker_uri_at_error}, Época: {tracker_epoch_at_error}). Considerando-o como falho.")

        if str(self.uri) == tracker_uri_at_error and self.is_tracker:
            self.logger.info("Eu era o tracker e detectei um problema. Parando atividades de tracker.")

        self._clear_current_tracker()
        # mantém current_tracker_epoch com o valor da época do tracker que falhou,
        # para que a próxima eleição seja para uma época superior.
        self.current_tracker_epoch = tracker_epoch_at_error
        self.initiate_election()

    # lógica de eleição
    def initiate_election(self):
        # começa processo de eleição para escolher novo tracker
        if self.election_vote_collection_timer and self.election_vote_collection_timer.is_alive():
            self.election_vote_collection_timer.cancel()

        max_known_epoch = self.current_tracker_epoch

        if self.voted_in_epoch:
            max_known_epoch = max(max_known_epoch, max(self.voted_in_epoch.keys(), default=-1))

        if hasattr(self, 'candidate_for_epoch_value_history') and self.candidate_for_epoch_value_history > 0:
            max_known_epoch = max(max_known_epoch, self.candidate_for_epoch_value_history)

        new_election_epoch = max_known_epoch + 1

        # se já sou candidato para esta nova época (improvável, mas para evitar loop)
        if self.candidate_for_epoch == 1 and self.candidate_for_epoch_value == new_election_epoch:
            self.logger.info(f"Já sou candidato para a época {new_election_epoch}. Aguardando resultado.")
            return

        self.candidate_for_epoch = 1  # sinaliza que estou ativamente buscando votos
        self.candidate_for_epoch_value = new_election_epoch
        self.candidate_for_epoch_value_history = new_election_epoch  # guarda a maior época tentada

        self.logger.info(f"Iniciando eleição para Tracker_Epoca_{self.candidate_for_epoch_value}.")

        # vota em si mesmo para a nova época da candidatura
        self.votes_received_for_epoch[self.candidate_for_epoch_value] = {str(self.uri)}
        self.voted_in_epoch[self.candidate_for_epoch_value] = str(self.uri)

        other_peer_uris = self._get_other_peer_uris()
        if not other_peer_uris and QUORUM <= 1:
            self.logger.info("Nenhum outro peer encontrado. Tentando me eleger sozinho (Quorum=1).")
            self._check_election_results()
            return
        elif not other_peer_uris:
            self.logger.info(f"Nenhum outro peer encontrado. Quorum é {QUORUM}, preciso de mais peers para me eleger.")
            # a eleição vai falhar no _check_election_results por falta de votos.
            # o timer de eleição irá expirar e chamar _check_election_results.
            pass  # continua para iniciar o timer de coleta

        self.logger.info(
            f"Solicitando votos de {len(other_peer_uris)} peers para época {self.candidate_for_epoch_value}.")
        for peer_uri_str in other_peer_uris:
            # não envia para si mesmo (já tratado em _get_other_peer_uris)
            try:

                # peer_proxy = Pyro5.api.Proxy(peer_uri_str)
                # peer_proxy._pyroTimeout = 2
                # passa self.candidate_for_epoch_value que é a época correta da eleição
                threading.Thread(target=self._send_vote_request_to_peer,
                                 args=(None, peer_uri_str, self.candidate_for_epoch_value)).start()
            except Exception as e:
                self.logger.error(f"Erro ao criar proxy para {peer_uri_str} durante eleição: {e}")

        if self.election_vote_collection_timer and self.election_vote_collection_timer.is_alive():
            self.election_vote_collection_timer.cancel()
        self.election_vote_collection_timer = threading.Timer(ELECTION_REQUEST_TIMEOUT, self._check_election_results)
        self.election_vote_collection_timer.daemon = True
        self.election_vote_collection_timer.start()

    def _send_vote_request_to_peer(self, peer_proxy, peer_uri_str, election_epoch_of_request):
        # monta proxy e solicita voto de outro peer
        local_peer_proxy = None
        try:
            local_peer_proxy = Pyro5.api.Proxy(peer_uri_str)
            local_peer_proxy._pyroTimeout = 2
            # verifica se ainda sou candidato para ESTA época específica antes de enviar
            if self.candidate_for_epoch == 0 or self.candidate_for_epoch_value != election_epoch_of_request:
                self.logger.info(
                    f"Minha candidatura para época {election_epoch_of_request} foi cancelada/mudou. Não solicitando voto de {peer_uri_str}.")
                return

            self.logger.info(f"Solicitando voto de {peer_uri_str} para época {election_epoch_of_request}")
            vote_granted = local_peer_proxy.request_vote(str(self.uri), election_epoch_of_request)

            # verifica novamente após o retorno, pois o estado pode ter mudado
            if self.candidate_for_epoch == 0 or self.candidate_for_epoch_value != election_epoch_of_request:
                self.logger.info(
                    f"Recebi resposta de voto de {peer_uri_str} para {election_epoch_of_request}, mas minha candidatura mudou/foi cancelada.")
                return

            if vote_granted:
                self.logger.info(f"Voto recebido de {peer_uri_str} para época {election_epoch_of_request}.")
                # garante que a estrutura de votos existe para esta época
                if election_epoch_of_request not in self.votes_received_for_epoch:
                    self.votes_received_for_epoch[election_epoch_of_request] = set()  # inicializa se não existir
                self.votes_received_for_epoch[election_epoch_of_request].add(peer_uri_str)
            else:
                self.logger.info(f"Voto negado por {peer_uri_str} para época {election_epoch_of_request}.")
        except Pyro5.errors.CommunicationError:
            self.logger.warning(
                f"Falha ao solicitar voto de {peer_uri_str} para época {election_epoch_of_request} (CommunicationError).")
        except Exception as e:
            self.logger.error(f"Erro ao solicitar voto de {peer_uri_str} para época {election_epoch_of_request}: {e}")

    @Pyro5.api.expose
    def request_vote(self, candidate_uri_str, election_epoch):
        # avalia pedido de voto e decide se concede ou nega
        self.logger.info(f"Pedido de voto recebido de {candidate_uri_str} para Tracker_Epoca_{election_epoch}.")
        self.logger.info(
            f"Meu estado: current_tracker_epoch={self.current_tracker_epoch}, voted_in_epoch[{election_epoch}]={self.voted_in_epoch.get(election_epoch)}, current_tracker_uri={self.current_tracker_uri_str}, sou_candidato_para_epoca={self.candidate_for_epoch_value if self.candidate_for_epoch else 'Nao'}")

        # regra 1: época da eleição vs época do tracker atual
        # nega se a eleição for para uma época estritamente menor que a do meu tracker ativo.
        if election_epoch < self.current_tracker_epoch and self.current_tracker_uri_str is not None:
            self.logger.info(
                f"Voto negado (Regra 1): época da eleição ({election_epoch}) é menor que a do tracker atual ({self.current_tracker_epoch}) que considero ativo.")
            return False

        # regra 2: época da eleição vs tracker ativo na mesma época
        # nega se a eleição for para a mesma época do meu tracker ativo, e o candidato não for esse tracker.
        if election_epoch == self.current_tracker_epoch and \
                self.current_tracker_uri_str is not None and \
                self.current_tracker_uri_str != candidate_uri_str:
            self.logger.info(
                f"Voto negado (Regra 2): época da eleição ({election_epoch}) é a mesma do tracker atual ({self.current_tracker_uri_str}) que considero ativo, e candidato é outro.")
            return False

        # regra 3: um voto por época, com desempate para candidatos na mesma época
        if election_epoch in self.voted_in_epoch:
            current_voted_candidate_in_epoch = self.voted_in_epoch[election_epoch]
            if current_voted_candidate_in_epoch == candidate_uri_str:
                self.logger.info(f"Já votei em {candidate_uri_str} para a época {election_epoch}. Confirmando voto.")
                return True

            # lógica de desempate: se eu votei em mim mesmo (porque sou/fui candidato para esta época)
            # e o novo candidato (candidate_uri_str) tem um URI "menor" (critério de desempate).
            if current_voted_candidate_in_epoch == str(self.uri) and \
                    candidate_uri_str != str(self.uri) and \
                    candidate_uri_str < str(self.uri):  # comparação de strings de URI
                self.logger.info(
                    f"Eu votei em mim ({str(self.uri)}) para época {election_epoch}, mas {candidate_uri_str} tem URI menor. Mudando meu voto para {candidate_uri_str}.")
                self.voted_in_epoch[election_epoch] = candidate_uri_str  # mudo meu voto

                # cancelo minha candidatura ATIVA para esta época se eu era candidato para ela
                if self.candidate_for_epoch == 1 and self.candidate_for_epoch_value == election_epoch:
                    self.logger.info(
                        f"Cancelando minha candidatura ativa para época {election_epoch} porque votei em {candidate_uri_str} (URI menor).")
                    if self.election_vote_collection_timer and self.election_vote_collection_timer.is_alive():
                        self.election_vote_collection_timer.cancel()
                    self.candidate_for_epoch = 0
                    self.votes_received_for_epoch.pop(election_epoch,
                                                      None)
                    # remove votos da minha candidatura cancelada
                    # self.candidate_for_epoch_value pode ser mantido como histórico ou zerado. Se zerado, zera candidate_for_epoch_value_history também.

                self._stop_tracker_timeout_detection()  # participei ativamente, paro de monitorar o antigo.
                return True  # voto concedido ao candidato com URI menor
            else:
                # já votei em mim e meu URI é menor/igual, ou já votei em outro que não o candidato atual.
                self.logger.info(
                    f"Voto negado (Regra 3b): já votei em {current_voted_candidate_in_epoch} para a época {election_epoch} e não mudarei (ou meu URI é menor/igual, ou já votei em terceiro).")
                return False

        # se não votei ainda nesta época (cheguei aqui porque election_epoch not in self.voted_in_epoch)
        # concedo o voto.
        self.voted_in_epoch[election_epoch] = candidate_uri_str
        self.logger.info(
            f"Voto concedido para {candidate_uri_str} para Tracker_Epoca_{election_epoch} (primeiro voto nesta época).")

        self._stop_tracker_timeout_detection()

        # se eu era candidato para uma época X <= election_epoch, e estou votando em election_epoch (e não sou eu), cancelo X.
        if self.candidate_for_epoch == 1 and \
                self.candidate_for_epoch_value > 0 and \
                self.candidate_for_epoch_value <= election_epoch and \
                str(self.uri) != candidate_uri_str:
            self.logger.info(
                f"Eu era candidato para época {self.candidate_for_epoch_value}, mas votei em {candidate_uri_str} para época {election_epoch}. Cancelando minha candidatura.")
            if self.election_vote_collection_timer and self.election_vote_collection_timer.is_alive():
                self.election_vote_collection_timer.cancel()
            self.candidate_for_epoch = 0
            self.votes_received_for_epoch.pop(self.candidate_for_epoch_value, None)

        return True

    def _check_election_results(self):
        # checa resultados da eleição e decide se virei tracker
        election_epoch_being_checked = self.candidate_for_epoch_value

        if self.candidate_for_epoch == 0 or election_epoch_being_checked == 0:
            self.logger.debug(
                f"Verificação de resultados de eleição, mas não sou candidato ativo ou a época da candidatura é 0 (época: {election_epoch_being_checked}).")
            return

        if election_epoch_being_checked not in self.votes_received_for_epoch:
            self.logger.info(f"Nenhum voto registrado para minha candidatura da época {election_epoch_being_checked}.")
            self.candidate_for_epoch = 0  # deixa de ser candidato ativo para esta tentativa
            return

        num_votes = len(self.votes_received_for_epoch.get(election_epoch_being_checked, set()))
        self.logger.info(
            f"Eleição para época {election_epoch_being_checked}: {num_votes} votos recebidos. Quórum necessário: {QUORUM}.")

        if num_votes >= QUORUM:
            self.logger.info(f"Quórum atingido! Eleito como Tracker_Epoca_{election_epoch_being_checked}.")
            self._become_tracker(election_epoch_being_checked)
        else:
            self.logger.info(
                f"Quórum não atingido para época {election_epoch_being_checked}. Eleição falhou para esta tentativa.")
            # não remove o voto de self.voted_in_epoch[election_epoch_being_checked] aqui, pois o voto foi dado.
            # apenas limpa os votos recebidos para esta tentativa de candidatura.
            self.votes_received_for_epoch.pop(election_epoch_being_checked, None)

        # independentemente do resultado, esta rodada de candidatura ativa terminou.
        self.candidate_for_epoch = 0
        # self.candidate_for_epoch_value é mantido em candidate_for_epoch_value_history para cálculo da próxima época.

    def _become_tracker(self, epoch):
        # assume o papel de tracker e registra no nameserver
        if self.is_tracker and self.current_tracker_epoch == epoch:
            self.logger.info(f"Já sou o tracker para a época {epoch}.")
            return  # já sou este tracker.

        self.logger.info(f"Tornando-me Tracker_Epoca_{epoch}.")
        ns_proxy_local_become = None
        try:
            ns_proxy_local_become = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
        except Pyro5.errors.NamingError:
            self.logger.error(f"Falha ao conectar ao servidor de nomes em _become_tracker. Não é possível registrar como tracker.")
            # não deve tentar _step_down_as_tracker() aqui se nunca se tornou um.
            # apenas limpa o estado local se necessário e retorna.
            self.is_tracker = False  # garante que não se considera tracker
            return

        # atualiza o estado interno para ser o tracker
        self.is_tracker = True
        self.current_tracker_epoch = epoch
        self.current_tracker_uri_str = str(self.uri)
        self.current_tracker_proxy = self  # referência local para mim mesmo como tracker

        # limpa dados de candidaturas e votos recebidos, pois agora sou tracker
        self.candidate_for_epoch = 0
        self.candidate_for_epoch_value = 0  # zera a época da candidatura ativa
        self.votes_received_for_epoch = {}
        # self.voted_in_epoch é um histórico, não limpar totalmente. mas para 'epoch', eu sou o tracker.

        tracker_name = f"{TRACKER_BASE_NAME}{epoch}"
        try:
            # tenta remover um registro obsoleto deste mesmo nome (se houver) antes de registrar o novo.
            try:
                existing_uri = ns_proxy_local_become.lookup(tracker_name)
                if existing_uri != self.uri:  # compara com self.uri (objeto URI)
                    self.logger.warning(
                        f"Já existe um registro para {tracker_name} com URI {existing_uri} (diferente do meu). Tentando remover e registrar o meu.")
                    ns_proxy_local_become.remove(tracker_name)  # tenta remover o antigo
                elif str(existing_uri) == str(self.uri):  # compara strings de URI
                    self.logger.info(f"Já estou registrado como {tracker_name}. Apenas confirmando.")

            except Pyro5.errors.NamingError:
                pass  # nome não existe, ótimo.

            ns_proxy_local_become.register(tracker_name, self.uri)
            self.logger.info(f"Registrado no servidor de nomes como {tracker_name} (URI: {self.uri}).")
        except Exception as e:
            self.logger.error(f"Falha ao registrar como {tracker_name} no servidor de nomes: {e}")
            self._step_down_as_tracker()  # renuncia se não conseguir se registrar
            return

        self.file_index = {}  # reinicia o índice de arquivos do tracker
        self._update_tracker_index_for_peer(self.peer_id, str(self.uri),
                                            self.local_files)  # adiciona meus próprios arquivos

        self._stop_tracker_timeout_detection()
        self._start_sending_heartbeats()

    def _step_down_as_tracker(self):
        # renuncia ao tracker e remove registro do servidor de nomes
        if not self.is_tracker:
            return

        epoch_i_was_tracker = self.current_tracker_epoch
        self.logger.info(f"Renunciando ao posto de Tracker_Epoca_{epoch_i_was_tracker}.")
        self._stop_sending_heartbeats()

        tracker_name = f"{TRACKER_BASE_NAME}{epoch_i_was_tracker}"
        ns_proxy_local_stepdown = None
        try:
            ns_proxy_local_stepdown = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
            # só remove se o URI no NS for o meu. evita remover registro de outro tracker.
            registered_uri = ns_proxy_local_stepdown.lookup(tracker_name)
            if str(registered_uri) == str(self.uri):  # compara strings de URI
                ns_proxy_local_stepdown.remove(tracker_name)
                self.logger.info(f"Removido {tracker_name} (meu registro) do servidor de nomes.")
            else:
                self.logger.info(f"Não removi {tracker_name} do NS, pois o URI ({registered_uri}) não é o meu.")
        except Pyro5.errors.NamingError:
            self.logger.info(
                f"{tracker_name} não encontrado no servidor de nomes para remoção (pode já ter sido substituído).")
        except Pyro5.errors.CommunicationError:
            self.logger.warning(f"Servidor de nomes inacessível em _step_down_as_tracker ao tentar remover {tracker_name}.")
        except Exception as e:
            self.logger.error(f"Erro ao tentar remover/verificar {tracker_name} do NS: {e}")

        # mantém current_tracker_epoch com o valor da época em que fui tracker,
        # para que _clear_current_tracker não a perca e a próxima descoberta/eleição use-a como base.
        self._clear_current_tracker()
        self.current_tracker_epoch = epoch_i_was_tracker  # restaura a época que eu era, para base da próxima eleição/descoberta

        self.logger.info("Após renunciar, vou tentar descobrir um novo tracker ou participar de eleição.")
        self._discover_tracker()

    # heartbeat e detecção de falha
    def _start_tracker_timeout_detection(self):
        # agenda timer para monitorar falhas do tracker
        if self.is_tracker:
            self._stop_tracker_timeout_detection()
            return

        self._stop_tracker_timeout_detection()

        if self.current_tracker_proxy and self.current_tracker_uri_str:  # só inicia se tiver um tracker definido
            timeout = random.uniform(TRACKER_DETECTION_TIMEOUT_MIN, TRACKER_DETECTION_TIMEOUT_MAX)
            self.tracker_timeout_timer = threading.Timer(timeout, self._handle_tracker_timeout)
            self.tracker_timeout_timer.daemon = True
            self.tracker_timeout_timer.start()
            self.logger.info(
                f"Timer de detecção de falha do tracker iniciado ({timeout:.2f}s) para {self.current_tracker_uri_str} (Época {self.current_tracker_epoch}).")
        else:
            self.logger.debug("Não iniciando timer de detecção de falha: nenhum tracker atual definido.")

    def _stop_tracker_timeout_detection(self):
        # cancela timer de detecção de falha
        if self.tracker_timeout_timer and self.tracker_timeout_timer.is_alive():
            self.tracker_timeout_timer.cancel()
            self.logger.debug("Timer de detecção de falha do tracker parado.")

    def _handle_tracker_timeout(self):
        # executa ao expirar timer de falha do tracker e inicia eleição
        if self.is_tracker: return  # se eu me tornei tracker enquanto o timer estava rodando

        # verifica se o tracker que deu timeout é realmente o que estávamos monitorando
        if self.current_tracker_proxy is None or self.current_tracker_uri_str is None:
            self.logger.info(
                "Timeout do tracker, mas já não há tracker atual definido (pode ter mudado). Ignorando este timeout.")
            return

        tracker_uri_timed_out = self.current_tracker_uri_str
        epoch_timed_out = self.current_tracker_epoch
        self.logger.warning(
            f"Timeout: Tracker {tracker_uri_timed_out} (Epoca {epoch_timed_out}) não respondeu (ou não enviou heartbeat válido).")

        self._clear_current_tracker()
        self.current_tracker_epoch = epoch_timed_out  # mantém a época do tracker que falhou como base

        self.initiate_election()

    def _start_sending_heartbeats(self):
        # começa a enviar heartbeats periodicamente se eu for tracker
        if not self.is_tracker:
            return

        self._stop_sending_heartbeats()

        def heartbeat_wrapper():
            if self.is_tracker:
                self._send_heartbeat_and_reschedule()
            else:
                self.logger.info("Não sou mais tracker, parando envio de heartbeats agendado.")
                self._heartbeat_logging_once = False  # reseta flag de log

        self.heartbeat_send_timer = threading.Timer(HEARTBEAT_INTERVAL, heartbeat_wrapper)
        self.heartbeat_send_timer.daemon = True
        self.heartbeat_send_timer.start()

        if not hasattr(self, '_heartbeat_logging_once') or not self._heartbeat_logging_once:
            self.logger.info(f"Tracker: Envio de heartbeats iniciado para época {self.current_tracker_epoch}.")
            self._heartbeat_logging_once = True

    def _stop_sending_heartbeats(self):
        # interrompe envio de heartbeats
        if self.heartbeat_send_timer and self.heartbeat_send_timer.is_alive():
            self.heartbeat_send_timer.cancel()
            self.logger.info("Tracker: Envio de heartbeats parado.")
            self._heartbeat_logging_once = False

    def _send_heartbeat_and_reschedule(self):
        # envia heartbeat e já prepara o próximo envio
        if not self.is_tracker:
            self.logger.info("Tentativa de enviar heartbeat, mas não sou mais tracker.")
            return

        self.logger.debug(f"Tracker: Enviando heartbeat da época {self.current_tracker_epoch}.")
        other_peer_uris = self._get_other_peer_uris()

        for peer_uri_str in other_peer_uris:
            # não envia para si mesmo (já filtrado em _get_other_peer_uris)
            try:
                # peer_proxy = Pyro5.api.Proxy(peer_uri_str)
                # peer_proxy._pyroTimeout = 0.5
                threading.Thread(target=self._safe_send_heartbeat_to_one_peer,
                                 args=(None, peer_uri_str, str(self.uri), self.current_tracker_epoch)).start()
            except Exception as e:
                self.logger.warning(f"Tracker: Falha ao criar proxy para {peer_uri_str} para enviar heartbeat: {e}")

        if self.is_tracker:
            self._start_sending_heartbeats()  # reagenda o próximo

    def _safe_send_heartbeat_to_one_peer(self, peer_proxy, target_peer_uri_str, tracker_uri_str, tracker_epoch):
        # tenta enviar heartbeat a um peer e captura falhas sem interromper o tracker
        local_target_proxy = None
        try:
            local_target_proxy = Pyro5.api.Proxy(target_peer_uri_str)
            local_target_proxy._pyroTimeout = 0.5
            local_target_proxy.receive_heartbeat(tracker_uri_str, tracker_epoch)
        except Pyro5.errors.CommunicationError:
            self.logger.debug(
                f"Tracker: Falha de comunicação ao enviar heartbeat para {target_peer_uri_str}. Peer pode estar offline.")
        except Exception as e:
            self.logger.warning(f"Tracker: Erro inesperado ao enviar heartbeat para {target_peer_uri_str}: {e}")

    @Pyro5.api.expose
    def receive_heartbeat(self, incoming_tracker_uri_str, incoming_tracker_epoch):
        # processa heartbeat recebido e decide se mantenho ou renuncio
        self.logger.debug(
            f"Heartbeat recebido de {incoming_tracker_uri_str} (Epoca {incoming_tracker_epoch}). Meu tracker: {self.current_tracker_uri_str} (Epoca {self.current_tracker_epoch}). Sou tracker: {self.is_tracker}")

        if self.is_tracker:  # se eu sou um tracker
            if incoming_tracker_uri_str == str(self.uri):  # heartbeat de mim mesmo (não deveria ocorrer)
                return
            # recebi heartbeat de OUTRO tracker. conflito!
            if incoming_tracker_epoch > self.current_tracker_epoch:
                self.logger.warning(
                    f"Sou Tracker (Época {self.current_tracker_epoch}), mas recebi heartbeat de {incoming_tracker_uri_str} (Época {incoming_tracker_epoch}). Época dele é MAIOR. Renunciando.")
                self._step_down_as_tracker()
                # _step_down_as_tracker chamará _discover_tracker, que deve encontrar o novo tracker.
                return
            elif incoming_tracker_epoch == self.current_tracker_epoch:
                # conflito na mesma época. desempate por URI.
                if incoming_tracker_uri_str < str(self.uri):
                    self.logger.warning(
                        f"Sou Tracker (Época {self.current_tracker_epoch}), mas recebi heartbeat de {incoming_tracker_uri_str} (MESMA época) com URI MENOR. Renunciando.")
                    self._step_down_as_tracker()
                    return
                else:  # Meu URI é menor ou igual, eu continuo.
                    self.logger.info(
                        f"Sou Tracker (Época {self.current_tracker_epoch}), recebi heartbeat de {incoming_tracker_uri_str} (MESMA época) com URI MAIOR/IGUAL. Ignorando o dele.")
                    return
            else:  # incoming_tracker_epoch < self.current_tracker_epoch
                self.logger.info(
                    f"Sou Tracker (Época {self.current_tracker_epoch}), recebi heartbeat de um tracker antigo/inferior ({incoming_tracker_uri_str}, Época {incoming_tracker_epoch}). Ignorando.")
                return

        # Se não sou tracker, processo o heartbeat:
        if incoming_tracker_epoch > self.current_tracker_epoch:
            self.logger.info(
                f"Novo tracker ou tracker com época superior detectado (Época {incoming_tracker_epoch} > {self.current_tracker_epoch}). Conectando a {incoming_tracker_uri_str}.")
            self._stop_tracker_timeout_detection()
            self.voted_in_epoch = {e: c for e, c in self.voted_in_epoch.items() if e >= incoming_tracker_epoch}
            self._connect_to_tracker(Pyro5.api.URI(incoming_tracker_uri_str), incoming_tracker_epoch)

        elif incoming_tracker_epoch == self.current_tracker_epoch:
            if self.current_tracker_uri_str is None:  # Não tinha tracker, aceito este.
                self.logger.info(
                    f"Recebi heartbeat de tracker {incoming_tracker_uri_str} para época {incoming_tracker_epoch} (eu não tinha tracker para esta época). Conectando.")
                self._connect_to_tracker(Pyro5.api.URI(incoming_tracker_uri_str), incoming_tracker_epoch)
            elif self.current_tracker_uri_str == incoming_tracker_uri_str:  # Heartbeat do meu tracker atual.
                self.logger.debug(
                    f"Heartbeat válido do tracker atual {self.current_tracker_uri_str}. Reiniciando timer de timeout.")
                self._start_tracker_timeout_detection()
            else:  # Tracker diferente, mesma época. Desempate por URI.
                if incoming_tracker_uri_str < self.current_tracker_uri_str:
                    self.logger.warning(
                        f"Heartbeat de tracker alternativo {incoming_tracker_uri_str} (época {incoming_tracker_epoch}) com URI MENOR que meu tracker atual {self.current_tracker_uri_str}. Mudando.")
                    self._stop_tracker_timeout_detection()
                    self._connect_to_tracker(Pyro5.api.URI(incoming_tracker_uri_str), incoming_tracker_epoch)
                else:  # Meu tracker atual tem URI menor/igual. Mantenho.
                    self.logger.info(
                        f"Heartbeat de tracker alternativo {incoming_tracker_uri_str} (época {incoming_tracker_epoch}), mas meu tracker atual {self.current_tracker_uri_str} tem URI menor/igual. Mantendo o atual e reiniciando timer.")
                    self._start_tracker_timeout_detection()  # Reinicia para o tracker ATUAL
        else:  # incoming_tracker_epoch < self.current_tracker_epoch
            self.logger.debug(
                f"Heartbeat de tracker antigo/inferior ({incoming_tracker_uri_str}, Época {incoming_tracker_epoch}) ignorado.")

    # --- Funcionalidades do Tracker (quando self.is_tracker == True) ---
    @Pyro5.api.expose
    def register_files(self, peer_id_req, peer_uri_str_req, file_list_req, peer_tracker_epoch_view_req):
        """Chamado por peers para registrar/atualizar seus arquivos no tracker."""
        if not self.is_tracker:
            self.logger.warning(
                f"Chamada para register_files ({peer_id_req}) recebida, mas não sou o tracker. Sou {self.peer_id} (época {self.current_tracker_epoch}). Tracker conhecido: {self.current_tracker_uri_str}")
            # Retorna informação sobre o tracker que este peer conhece, se houver
            return {"status": "not_tracker",
                    "known_tracker_uri": self.current_tracker_uri_str,
                    "known_tracker_epoch": self.current_tracker_epoch}

        if peer_tracker_epoch_view_req < self.current_tracker_epoch:
            self.logger.warning(
                f"Tracker: Peer {peer_id_req} (URI {peer_uri_str_req}) tentou registrar com época antiga ({peer_tracker_epoch_view_req} vs minha {self.current_tracker_epoch}). Instruindo a atualizar.")
            return {"status": "epoch_too_low", "current_tracker_epoch": self.current_tracker_epoch}

        self.logger.info(
            f"Tracker: {peer_id_req} ({peer_uri_str_req}) registrando/atualizando arquivos (peer viu época {peer_tracker_epoch_view_req}): {file_list_req}")
        self._update_tracker_index_for_peer(peer_id_req, peer_uri_str_req, file_list_req)
        return {"status": "ok", "registered_at_epoch": self.current_tracker_epoch}

    def _update_tracker_index_for_peer(self, peer_id_to_update, peer_uri_to_update, new_file_list):
        """Lógica interna para atualizar o índice de arquivos para um peer específico."""
        # Remove todas as entradas antigas para este peer_id_to_update
        for filename in list(self.file_index.keys()):
            current_holders = self.file_index.get(filename, set())
            # Filtra para remover o peer_id_to_update, mantendo outros
            updated_holders = {(pid, puri) for pid, puri in current_holders if pid != peer_id_to_update}

            if not updated_holders:  # Se o set ficar vazio
                del self.file_index[filename]
            elif len(updated_holders) < len(current_holders):  # Se algo foi removido
                self.file_index[filename] = updated_holders

        # Adiciona os novos arquivos da new_file_list
        for filename in new_file_list:
            if filename not in self.file_index:
                self.file_index[filename] = set()
            self.file_index[filename].add((peer_id_to_update, peer_uri_to_update))
        self.logger.debug(f"Tracker: Índice atualizado para {peer_id_to_update}. Índice agora: {self.file_index}")

    @Pyro5.api.expose
    def query_file(self, filename_req, asking_peer_epoch_view_req):
        """Chamado por peers para perguntar quem tem um arquivo."""
        if not self.is_tracker:
            # self.logger.warning("Chamada para query_file recebida, mas não sou o tracker.")
            return {"status": "not_tracker",
                    "known_tracker_uri": self.current_tracker_uri_str,
                    "known_tracker_epoch": self.current_tracker_epoch,
                    "holders": []}

        if asking_peer_epoch_view_req < self.current_tracker_epoch:
            self.logger.warning(
                f"Tracker: Peer com época desatualizada ({asking_peer_epoch_view_req}) tentou consultar arquivo '{filename_req}'. Minha época: {self.current_tracker_epoch}")
            return {"status": "epoch_too_low", "current_tracker_epoch": self.current_tracker_epoch, "holders": []}

        self.logger.info(
            f"Tracker: Consulta pelo arquivo '{filename_req}' (peer viu época {asking_peer_epoch_view_req}).")
        holders = list(self.file_index.get(filename_req, set()))
        self.logger.info(f"Tracker: Arquivo '{filename_req}' encontrado nos peers: {holders}")
        return {"status": "ok", "holders": holders}

    @Pyro5.api.expose
    def get_all_indexed_files(self, asking_peer_epoch_view_req):
        """Retorna um dicionário de todos os arquivos indexados e quem os possui."""
        if not self.is_tracker:
            # self.logger.warning("Chamada para get_all_indexed_files recebida, mas não sou o tracker.")
            return {"status": "not_tracker",
                    "known_tracker_uri": self.current_tracker_uri_str,
                    "known_tracker_epoch": self.current_tracker_epoch,
                    "index": {}}

        if asking_peer_epoch_view_req < self.current_tracker_epoch:
            self.logger.warning(
                f"Tracker: Peer com época desatualizada ({asking_peer_epoch_view_req}) tentou listar todos os arquivos.")
            return {"status": "epoch_too_low", "current_tracker_epoch": self.current_tracker_epoch, "index": {}}

        serializable_index = {filename: list(peers) for filename, peers in self.file_index.items()}
        return {"status": "ok", "index": serializable_index}

    @Pyro5.api.expose
    def ping(self):
        """Método simples para verificar se o tracker (ou qualquer peer) está vivo."""
        self.logger.debug(f"Ping recebido em {self.peer_id} (URI: {self.uri})")
        return True

    # --- Funcionalidades do Peer (para transferência P2P) ---
    @Pyro5.api.expose
    def request_file_chunk(self, filename, chunk_offset, chunk_size):
        """Chamado por outro peer para baixar um chunk de um arquivo."""
        if filename not in self.local_files:
            self.logger.warning(f"Pedido de arquivo '{filename}' recebido, mas não o possuo.")
            return None

        file_path = os.path.join(self.shared_folder, filename)
        try:
            with open(file_path, 'rb') as f:
                f.seek(chunk_offset)
                data = f.read(chunk_size)
                self.logger.debug(f"Enviando chunk de '{filename}', offset {chunk_offset}, size {len(data)}")
                return data
        except Exception as e:
            self.logger.error(f"Erro ao ler arquivo '{filename}' para download: {e}")
            return None

    @Pyro5.api.expose
    def get_file_size(self, filename):
        """Retorna o tamanho de um arquivo local."""
        if filename not in self.local_files:
            return -1

        file_path = os.path.join(self.shared_folder, filename)
        try:
            return os.path.getsize(file_path)
        except Exception as e:
            self.logger.error(f"Erro ao obter tamanho do arquivo {filename}: {e}")
            return -1

    def _get_other_peer_uris(self):
        """Busca URIs de outros peers no servidor de nomes, excluindo o próprio URI."""
        try:
            self.ns_proxy = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
            peers_map = self.ns_proxy.list(prefix=PEER_NAME_PREFIX)
            return [uri for name, uri in peers_map.items() if uri != str(self.uri)]
        except Exception as e:
            self.logger.error(f"Erro ao listar outros peers no servidor de nomes: {e}")
            return []

    # --- Interface de Usuário (CLI) ---
    def _handle_tracker_response_for_cli(self, response, operation_name="operação"):
        """Função auxiliar para tratar respostas comuns do tracker na CLI."""
        if not response:  # Pode ser None devido a CommunicationError antes da chamada
            self.logger.error(f"Nenhuma resposta do tracker para {operation_name}.")
            return None

        status = response.get("status")
        if status == "epoch_too_low":
            new_epoch = response.get('current_tracker_epoch', self.current_tracker_epoch + 1)
            self.logger.warning(
                f"Minha visão da época do tracker ({self.current_tracker_epoch}) está desatualizada para {operation_name}. Tracker atual é época {new_epoch}. Tentando reconectar/descobrir...")
            self.current_tracker_epoch = new_epoch - 1  # Para que _discover_tracker tente a época correta ou maior
            self._discover_tracker()
            return None
        elif status == "not_tracker":
            self.logger.warning(
                f"O peer contatado não é o tracker para {operation_name}. Tracker conhecido por ele: {response.get('known_tracker_uri')} (Época {response.get('known_tracker_epoch')}). Tentando descobrir o tracker correto...")
            # Atualiza a época conhecida se a informação do "não tracker" for mais recente
            if response.get('known_tracker_epoch', -1) > self.current_tracker_epoch:
                self.current_tracker_epoch = response.get('known_tracker_epoch')
                self.current_tracker_uri_str = response.get('known_tracker_uri')  # Tenta conectar a este
            self._discover_tracker()
            return None
        elif status != "ok":
            self.logger.error(f"Erro na {operation_name} com o tracker: {response}")
            return None
        return response  # Retorna a resposta original se status "ok"

    def cli_search_file(self):
        filename = input("Digite o nome do arquivo para buscar: ")
        if not self.current_tracker_uri_str and not self.is_tracker: # Verifique current_tracker_uri_str
            self.logger.info("Nenhum tracker ativo conhecido. Tentando descobrir...")
            self._discover_tracker()
            if not self.current_tracker_uri_str and not self.is_tracker: # Verifique current_tracker_uri_str novamente
                self.logger.info("Ainda não há tracker ativo após nova tentativa de descoberta.")
                return

        raw_response = None
        if self.is_tracker:  # Se eu sou o tracker
            self.logger.info(f"Consultando meu próprio índice (sou o tracker) por '{filename}'...")
            raw_response = self.query_file(filename, self.current_tracker_epoch)
        elif self.current_tracker_uri_str: # Se tenho um URI de tracker
            try:
                # Criar proxy localmente para a chamada da CLI
                tracker_proxy_local = Pyro5.api.Proxy(self.current_tracker_uri_str)
                tracker_proxy_local._pyroTimeout = 5 # Definir timeout
                self.logger.info(
                    f"Consultando tracker {self.current_tracker_uri_str} (Época {self.current_tracker_epoch}) por '{filename}'...")
                raw_response = tracker_proxy_local.query_file(filename, self.current_tracker_epoch)
            except Pyro5.errors.CommunicationError:
                self.logger.error("Falha de comunicação com o tracker ao buscar arquivo.")
                self._handle_tracker_communication_error()
                return
            except Exception as e:
                self.logger.error(f"Erro ao buscar arquivo no tracker: {e}")
                return
        else:  # current_tracker_uri_str é None e não sou tracker
            self.logger.info("Não foi possível determinar um tracker para consultar.")
            return

        response = self._handle_tracker_response_for_cli(raw_response, "busca de arquivo")
        if not response: return

        holders = response.get("holders", [])
        if holders:
            self.logger.info(f"Arquivo '{filename}' encontrado nos seguintes peers:")
            for i, (holder_id, holder_uri_str) in enumerate(holders):
                print(f"  {i + 1}. Peer ID: {holder_id} (URI: {holder_uri_str})")

            choice = input("Deseja baixar? (s/n) ou escolha o número do peer: ")
            if choice.lower() == 's' or choice.isdigit():
                target_peer_index = 0
                if choice.isdigit() and 0 < int(choice) <= len(holders):
                    target_peer_index = int(choice) - 1

                chosen_peer_id, chosen_peer_uri_str = holders[target_peer_index]
                if str(self.uri) == chosen_peer_uri_str:
                    self.logger.info("Este peer já possui o arquivo localmente.")
                    return

                download_folder = os.path.join(os.getcwd(), "p2p_download_folders", self.peer_id)
                os.makedirs(download_folder, exist_ok=True)
                self._download_file_from_peer(filename, chosen_peer_uri_str, download_folder)
        else:
            self.logger.info(f"Arquivo '{filename}' não encontrado na rede (segundo o tracker).")

    def _download_file_from_peer(self, filename, target_peer_uri_str, download_folder):
        """Baixa um arquivo de outro peer em chunks."""
        try:
            target_peer_proxy = Pyro5.api.Proxy(target_peer_uri_str)
            target_peer_proxy._pyroTimeout = 10

            total_size = target_peer_proxy.get_file_size(filename)
            if total_size == -1:
                self.logger.error(
                    f"Arquivo '{filename}' não encontrado ou erro ao obter tamanho no peer de origem {target_peer_uri_str}.")
                return
            if total_size == 0:  # Arquivo vazio
                self.logger.info(f"Arquivo '{filename}' está vazio. Criando arquivo vazio localmente.")
                save_path = os.path.join(download_folder, filename)
                open(save_path, 'wb').close()  # Cria arquivo vazio
                print("\nDownload concluído (arquivo vazio)!")
                self.update_local_files_and_notify_tracker()
                return

            self.logger.info(f"Iniciando download de '{filename}' ({total_size} bytes) de {target_peer_uri_str}...")
            save_path = os.path.join(download_folder, filename)

            bytes_downloaded = 0
            with open(save_path, 'wb') as f:
                while bytes_downloaded < total_size:
                    chunk_data = target_peer_proxy.request_file_chunk(filename, bytes_downloaded, DOWNLOAD_CHUNK_SIZE)
                    if not chunk_data:
                        if bytes_downloaded < total_size:
                            self.logger.error(
                                f"Erro ao baixar chunk de '{filename}' (chunk vazio/None recebido antes do fim). Download interrompido.")
                            if os.path.exists(save_path): os.remove(save_path)
                            return
                        else:
                            break

                    f.write(chunk_data)
                    bytes_downloaded += len(chunk_data)
                    progress = (bytes_downloaded / total_size) * 100 if total_size > 0 else 100
                    print(f"\rBaixando '{filename}': {bytes_downloaded}/{total_size} bytes ({progress:.2f}%)", end="")
            print("\nDownload concluído!")
            self.logger.info(f"Arquivo '{filename}' baixado para {save_path}.")
            self.update_local_files_and_notify_tracker()

        except Pyro5.errors.CommunicationError:
            self.logger.error(f"Falha de comunicação com {target_peer_uri_str} durante o download.")
        except Exception as e:
            self.logger.error(f"Erro ao baixar arquivo '{filename}' de {target_peer_uri_str}: {e}")

    def cli_list_my_files(self):
        self.logger.info(f"Meus arquivos compartilhados ({len(self.local_files)}):")
        if not self.local_files:
            print("  (Nenhum arquivo local compartilhado)")
        for f_name in self.local_files:
            print(f"  - {f_name}")

    def cli_list_network_files(self):
        if not self.current_tracker_uri_str and not self.is_tracker: # Verifique current_tracker_uri_str
            self.logger.info("Nenhum tracker ativo conhecido. Tentando descobrir...")
            self._discover_tracker()
            if not self.current_tracker_uri_str and not self.is_tracker: # Verifique current_tracker_uri_str novamente
                self.logger.info("Ainda não há tracker ativo após nova tentativa de descoberta.")
                return

        raw_response = None
        if self.is_tracker:
            self.logger.info("Consultando meu próprio índice (sou o tracker) por todos os arquivos...")
            raw_response = self.get_all_indexed_files(self.current_tracker_epoch)
        elif self.current_tracker_uri_str: # Se tenho um URI de tracker
            try:
                # Criar proxy localmente para a chamada da CLI
                tracker_proxy_local = Pyro5.api.Proxy(self.current_tracker_uri_str)
                tracker_proxy_local._pyroTimeout = 5 # Definir timeout
                self.logger.info(
                    f"Consultando tracker {self.current_tracker_uri_str} (Época {self.current_tracker_epoch}) por todos os arquivos da rede...")
                raw_response = tracker_proxy_local.get_all_indexed_files(self.current_tracker_epoch)
            except Pyro5.errors.CommunicationError:
                self.logger.error("Falha de comunicação com o tracker ao listar arquivos da rede.")
                self._handle_tracker_communication_error()
                return
            except Exception as e:
                self.logger.error(f"Erro ao listar arquivos da rede no tracker: {e}")
                return
        else: # current_tracker_uri_str é None e não sou tracker
            self.logger.info("Não foi possível determinar um tracker para consultar.")
            return

        response = self._handle_tracker_response_for_cli(raw_response, "listagem de arquivos da rede")
        if not response: return

        network_index = response.get("index", {})
        if network_index:
            self.logger.info("Arquivos disponíveis na rede:")
            for filename, holders in network_index.items():
                holder_ids = [pid for pid, puri in holders]
                print(f"  - {filename} (disponível em: {', '.join(holder_ids)})")
        else:
            self.logger.info("Nenhum arquivo encontrado na rede ou tracker vazio.")

    def cli_refresh_local_files(self):
        self.logger.info("Verificando arquivos locais e notificando tracker (se aplicável)...")
        self.update_local_files_and_notify_tracker()
        self.cli_list_my_files()

    def cli_status(self):
        status_msg = f"\n--- Status do Peer {self.peer_id} ---"
        status_msg += f"\nURI: {self.uri}"
        status_msg += f"\nÉ Tracker: {'Sim' if self.is_tracker else 'Não'}"
        if self.is_tracker:
            status_msg += f"\nTracker Época Atual (Minha): {self.current_tracker_epoch}"
            status_msg += f"\nÍndice de Arquivos do Tracker ({len(self.file_index)} arquivos distintos):"
            if not self.file_index:
                status_msg += " Vazio"
            else:
                for fname, fholders in self.file_index.items():
                    status_msg += f"\n  - {fname}: {[pid for pid, _ in fholders]}"
        else:  # Não sou tracker
            status_msg += f"\nTracker Atual URI: {self.current_tracker_uri_str if self.current_tracker_uri_str else 'Nenhum conhecido'}"
            status_msg += f"\nTracker Atual Época (Conhecida): {self.current_tracker_epoch if self.current_tracker_uri_str else 'N/A'}"

        status_msg += f"\nMeus Arquivos Locais ({len(self.local_files)}): {self.local_files if self.local_files else 'Nenhum'}"

        voted_info_list = []
        for ep, cand_uri in self.voted_in_epoch.items():
            try:  # Tenta extrair ID do peer do URI para legibilidade
                cand_peer_id_part = cand_uri.split('@')[0].replace(PEER_NAME_PREFIX,
                                                                   '') if '@' in cand_uri else cand_uri[-6:]
                voted_info_list.append(f"E{ep}:P({cand_peer_id_part})")
            except:
                voted_info_list.append(f"E{ep}:{cand_uri[-6:]}")

        voted_info = ", ".join(voted_info_list) if voted_info_list else "Nenhum voto registrado"
        status_msg += f"\nHistórico de Votos (Época:Candidato): {voted_info}"

        active_cand_epoch = self.candidate_for_epoch_value if self.candidate_for_epoch == 1 else 0
        status_msg += f"\nSou candidato ativo para Época: {active_cand_epoch if active_cand_epoch > 0 else 'Não'}"
        if active_cand_epoch > 0 and active_cand_epoch in self.votes_received_for_epoch:
            status_msg += f"\nVotos recebidos para minha candidatura (época {active_cand_epoch}): {len(self.votes_received_for_epoch[active_cand_epoch])} de {QUORUM} necessários"
        status_msg += "\n-------------------------"
        print(status_msg)

    def run_cli(self):
        """Loop principal da interface de linha de comando."""
        time.sleep(random.uniform(2, 4))

        print(f"\n--- CLI do Peer {self.peer_id} ---")
        print("Comandos disponíveis:")
        print("  search    - Buscar um arquivo na rede e opção de download")
        print("  list my   - Listar meus arquivos compartilhados")
        print("  list net  - Listar todos os arquivos na rede (via tracker)")
        print("  refresh   - Re-escanear pasta local e notificar tracker")
        print("  status    - Mostrar status atual do peer e do tracker")
        print("  election  - Forçar início de uma eleição (simula falha do tracker)")
        print("  quit      - Sair")

        while True:
            try:
                cmd = input(f"[{self.peer_id}@p2p]$ ").strip().lower()
                if cmd == "search":
                    self.cli_search_file()
                elif cmd == "list my":
                    self.cli_list_my_files()
                elif cmd == "list net":
                    self.cli_list_network_files()
                elif cmd == "refresh":
                    self.cli_refresh_local_files()
                elif cmd == "status":
                    self.cli_status()
                elif cmd == "election":
                    self.logger.info("Iniciando eleição manualmente (simulando falha do tracker)...")
                    if self.is_tracker:
                        self.logger.info("Eu sou o tracker. Vou renunciar para permitir nova eleição.")
                        self._step_down_as_tracker()
                    else:  # Não sou tracker, simulo que o tracker que conheço falhou
                        tracker_uri_at_cmd = self.current_tracker_uri_str
                        epoch_at_cmd = self.current_tracker_epoch
                        self._clear_current_tracker()
                        self.current_tracker_epoch = epoch_at_cmd  # Mantém a época do tracker "falho"
                        self.logger.info(f"Simulando falha do tracker {tracker_uri_at_cmd} (Época {epoch_at_cmd}).")
                        self.initiate_election()
                elif cmd == "quit":
                    self.logger.info("Saindo...")
                    break
                else:
                    if cmd: print("Comando desconhecido.")
            except EOFError:
                self.logger.warning("EOF recebido, encerrando CLI.")
                break
            except KeyboardInterrupt:
                self.logger.info("Interrupção de teclado na CLI. Use 'quit' para sair do peer.")
            except Exception as e:
                self.logger.error(f"Erro no CLI: {e}", exc_info=True)

    def start(self):
        """Inicia o peer: configura PyRO, descobre tracker e inicia loop do daemon."""
        self._setup_pyro()

        initial_delay = random.uniform(0.5, 2.0)
        if self.peer_id == "Peer1":  # Peer1 tenta ser o primeiro mais rapidamente
            initial_delay = random.uniform(0.1, 0.3)

        self.logger.info(f"Aguardando {initial_delay:.2f}s antes de descobrir o tracker...")
        time.sleep(initial_delay)

        self._discover_tracker()

        cli_thread = threading.Thread(target=self.run_cli, name=f"CLIThread-{self.peer_id}", daemon=True)
        cli_thread.start()

        self.logger.info(f"Peer {self.peer_id} pronto e aguardando requisições/comandos.")
        try:
            self.pyro_daemon.requestLoop()
        except KeyboardInterrupt:
            self.logger.info(f"Interrupção de teclado recebida no daemon do Peer {self.peer_id}. Encerrando...")
        finally:
            self.shutdown()

    def shutdown(self):
        """Encerra o peer de forma limpa."""
        self.logger.info(f"Encerrando Peer {self.peer_id}...")

        self._stop_tracker_timeout_detection()
        self._stop_sending_heartbeats()
        if self.election_vote_collection_timer and self.election_vote_collection_timer.is_alive():
            self.election_vote_collection_timer.cancel()
            self.logger.debug("Timer de coleta de votos da eleição cancelado.")

        if self.pyro_daemon:  # Garante que o daemon existe antes de tentar desligá-lo
            self.pyro_daemon.shutdown()

        if self.ns_proxy:
            try:
                self.ns_proxy.remove(f"{PEER_NAME_PREFIX}{self.peer_id}")
                self.logger.info(f"Removido {PEER_NAME_PREFIX}{self.peer_id} do servidor de nomes.")
                if self.is_tracker:
                    tracker_name_to_remove = f"{TRACKER_BASE_NAME}{self.current_tracker_epoch}"
                    # Apenas remove se o URI no NS for o meu
                    try:
                        registered_uri = self.ns_proxy.lookup(tracker_name_to_remove)
                        if registered_uri == self.uri:
                            self.ns_proxy.remove(tracker_name_to_remove)
                            self.logger.info(f"Removido {tracker_name_to_remove} (meu registro de tracker) do NS.")
                        else:
                            self.logger.info(
                                f"Não removi {tracker_name_to_remove} do NS, URI ({registered_uri}) não é meu.")
                    except Pyro5.errors.NamingError:
                        self.logger.debug(f"{tracker_name_to_remove} não encontrado no NS para remoção (tracker).")

            except Pyro5.errors.NamingError:
                self.logger.debug(
                    "Nomes não encontrados no servidor de nomes para remoção (podem já ter sido removidos).")
            except Pyro5.errors.CommunicationError:
                self.logger.warning("Falha de comunicação com o servidor de nomes durante o desregistro.")
            except Exception as e:
                self.logger.warning(f"Erro ao desregistrar do servidor de nomes: {e}")

        self.logger.info(f"Peer {self.peer_id} finalizado.")


# --- Ponto de Entrada Principal ---
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python peer.py <peer_id> <shared_folder_path>")
        print("Exemplo: python peer.py Peer1 ./p2p_shared_folders/peer1_files")
        sys.exit(1)

    peer_id_arg = sys.argv[1]
    shared_folder_arg = sys.argv[2]

    Pyro5.config.SERIALIZER = "serpent"
    Pyro5.config.DETAILED_TRACEBACK = True

    peer_instance = Peer(peer_id_arg, shared_folder_arg)
    try:
        peer_instance.start()
    except Exception as main_exc:  # Captura exceções não tratadas que podem parar o peer
        peer_instance.logger.critical(f"Erro crítico no Peer {peer_id_arg} que causou a sua paragem: {main_exc}",
                                      exc_info=True)
    finally:
        # Garante que o shutdown seja chamado mesmo se start() levantar uma exceção não tratada internamente,
        # ou se o loop do daemon for interrompido.
        if hasattr(peer_instance, 'pyro_daemon') and peer_instance.pyro_daemon and \
                hasattr(peer_instance.pyro_daemon, 'transportServer') and peer_instance.pyro_daemon.transportServer:
            peer_instance.logger.info(
                f"Chamando shutdown final para Peer {peer_id_arg} a partir do bloco finally principal.")
            # peer_instance.shutdown() # Chamado automaticamente pelo finally do start()
        else:
            peer_instance.logger.info(
                f"Bloco finally principal para Peer {peer_id_arg}, mas o daemon PyRO pode não ter iniciado completamente ou já foi encerrado.")

