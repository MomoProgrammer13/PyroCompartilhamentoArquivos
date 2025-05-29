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

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(peer_id)s - %(message)s')


@Pyro5.api.expose
@Pyro5.api.behavior(instance_mode="single")
class Peer:
    def __init__(self, peer_id, shared_folder_path):
        self.peer_id = peer_id

        # Configuração do logging para o arquivo deste peer
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)  # Cria o diretório de logs caso não exista
        self.log_file_path = os.path.join(log_dir, f"{self.peer_id}_app.log")

        # Remove handlers antigos do logger raiz para evitar logs duplicados
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(peer_id)s - %(message)s',
            filename=self.log_file_path,
            filemode='w'  # Sobrescreve o log a cada execução
        )

        self.logger = logging.LoggerAdapter(logging.getLogger(__name__), {'peer_id': self.peer_id})

        self.uri = None
        self.pyro_daemon = None

        self.shared_folder = os.path.abspath(shared_folder_path)
        os.makedirs(self.shared_folder, exist_ok=True)
        # self.local_files é inicializado com os arquivos atuais.
        # Ele será a "foto" do estado anterior para a próxima verificação.
        self.local_files = self._scan_local_files()

        self.is_tracker = False
        self.current_tracker_uri_str = None
        self.current_tracker_proxy = None
        self.current_tracker_epoch = 0

        # Atributos de eleição
        self.candidate_for_epoch = 0
        self.candidate_for_epoch_value = 0
        self.votes_received_for_epoch = {}
        self.voted_in_epoch = {}
        self.candidate_for_epoch_value_history = 0  # Adicionado para rastrear a maior época tentada

        # Timers
        self.tracker_timeout_timer = None
        self.heartbeat_send_timer = None
        self.election_vote_collection_timer = None
        self.ns_proxy = None  # Proxy do NameServer para uso geral

        self.logger.info(f"Peer inicializado. Pasta de compartilhamento: {self.shared_folder}")
        self.logger.info(f"Arquivos locais iniciais: {self.local_files}")

    def _setup_pyro(self):
        # Configura o daemon Pyro e faz o registro no servidor de nomes
        try:
            self.pyro_daemon = Pyro5.server.Daemon(host=self._get_local_ip())
            self.uri = self.pyro_daemon.register(self)
            # Obter um proxy NS local para registro inicial
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
        # Tenta descobrir o ip local para rodar o daemon
        return "localhost"  # Ou use uma lógica mais sofisticada para descobrir o IP

    def _scan_local_files(self):
        # Varre a pasta compartilhada e retorna lista de arquivos locais
        try:
            return [f for f in os.listdir(self.shared_folder) if os.path.isfile(os.path.join(self.shared_folder, f))]
        except Exception as e:
            self.logger.error(f"Erro ao escanear arquivos locais: {e}")
            return []

    def update_local_files_and_notify_tracker(self):
        # Atualiza lista de arquivos locais e avisa o tracker sobre mudanças

        # `self.local_files` contém os arquivos da última varredura (estado "antigo")
        old_files_set = set(self.local_files)

        # Varre a pasta para obter o estado atual dos arquivos
        current_files_list = self._scan_local_files()
        current_files_set = set(current_files_list)

        # Verifica se houve alguma mudança (adição ou remoção)
        if old_files_set != current_files_set:
            self.logger.info(
                f"Mudança nos arquivos locais detectada. Antigos: {old_files_set}, Atuais: {current_files_set}")

            # Calcula os arquivos que foram adicionados desde a última varredura
            added_files = list(current_files_set - old_files_set)
            # Calcula os arquivos que foram removidos (para logging ou futuras implementações)
            removed_files = list(old_files_set - current_files_set)

            # Atualiza a lista principal de arquivos do peer para o estado atual
            self.local_files = current_files_list

            if added_files:
                self.logger.info(f"Novos arquivos adicionados localmente: {added_files}")
            if removed_files:
                self.logger.info(
                    f"Arquivos removidos localmente: {removed_files}. O tracker não será notificado dessas remoções nesta atualização incremental.")

            # Se este peer não for o tracker e tiver um tracker conhecido, notifica sobre os NOVOS arquivos
            if self.current_tracker_uri_str and not self.is_tracker:
                if added_files:  # Só notifica se houver arquivos realmente novos para adicionar
                    try:
                        tracker_proxy_local = Pyro5.api.Proxy(self.current_tracker_uri_str)
                        tracker_proxy_local._pyroTimeout = 5
                        self.logger.info(
                            f"Notificando tracker {self.current_tracker_uri_str} (Época {self.current_tracker_epoch}) sobre NOVOS arquivos: {added_files}.")

                        # Envia APENAS os arquivos adicionados e informa que é uma atualização incremental
                        response = tracker_proxy_local.register_files(
                            self.peer_id,
                            str(self.uri),
                            added_files,  # Envia somente os arquivos novos
                            self.current_tracker_epoch,
                            is_incremental_update=True  # Novo parâmetro para indicar atualização incremental
                        )
                        if isinstance(response, dict) and response.get("status") == "epoch_too_low":
                            self.logger.warning(
                                f"Tracker informou que minha época ({self.current_tracker_epoch}) é muito baixa ao registrar novos arquivos. Tracker atual é época {response.get('current_tracker_epoch')}. Tentando reconectar/descobrir.")
                            self._discover_tracker()
                    except Pyro5.errors.CommunicationError:
                        self.logger.warning(
                            "Falha ao notificar tracker sobre novos arquivos (CommunicationError). Tracker pode estar offline.")
                        self._handle_tracker_communication_error()
                    except Exception as e:
                        self.logger.error(f"Erro ao notificar tracker sobre novos arquivos: {e}")
                else:
                    self.logger.info(
                        "Nenhum arquivo novo para adicionar ao tracker. Apenas remoções ou nenhuma mudança que necessite de adição incremental.")

            # Se este peer for o tracker, ele atualiza seu próprio índice completamente
            # (pois a mudança pode ser uma remoção, e a lógica incremental atual só adiciona)
            elif self.is_tracker:
                self.logger.info("Atualizando índice do tracker para meus próprios arquivos (mudança detectada).")
                # Um tracker sempre faz uma atualização completa para seus próprios arquivos
                # para lidar corretamente com adições e remoções.
                self._update_tracker_index_for_peer(self.peer_id, str(self.uri), self.local_files, is_incremental=False)
        else:
            self.logger.debug("Nenhuma mudança nos arquivos locais desde a última verificação.")

    def _discover_tracker(self):
        # Busca um tracker ativo no servidor de nomes e conecta a ele
        self.logger.info("Procurando por um tracker ativo...")
        latest_epoch_found = -1
        tracker_uri_to_connect = None
        ns_proxy_local_discover = None

        try:
            ns_proxy_local_discover = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
        except Pyro5.errors.NamingError:
            self.logger.error(f"Servidor de nomes não encontrado em _discover_tracker.")
            pass

        if ns_proxy_local_discover:
            for i in range(MAX_EPOCH_SEARCH, -1, -1):
                tracker_name_to_find = f"{TRACKER_BASE_NAME}{i}"
                uri_obj_from_ns = None
                try:
                    uri_obj_from_ns = ns_proxy_local_discover.lookup(tracker_name_to_find)
                    if uri_obj_from_ns:
                        temp_proxy = Pyro5.api.Proxy(str(uri_obj_from_ns))
                        temp_proxy._pyroTimeout = 1.5
                        temp_proxy.ping()

                        latest_epoch_found = i
                        tracker_uri_to_connect = uri_obj_from_ns
                        self.logger.info(
                            f"Tracker encontrado: {tracker_name_to_find} com URI {uri_obj_from_ns} (Época {i}).")
                        break
                except Pyro5.errors.NamingError:
                    continue
                except Pyro5.errors.CommunicationError:
                    self.logger.warning(
                        f"Tracker {tracker_name_to_find} (URI {uri_obj_from_ns if uri_obj_from_ns else 'desconhecido'}) encontrado no NS, mas não respondeu ao ping. Considerando-o falho.")
                    if uri_obj_from_ns:  # Tenta remover do NS se não responde
                        try:
                            ns_proxy_local_discover.remove(tracker_name_to_find)
                            self.logger.info(
                                f"Tracker {tracker_name_to_find} (URI {uri_obj_from_ns}) removido do NS por não responder.")
                        except Exception as e_remove:
                            self.logger.warning(
                                f"Falha ao tentar remover tracker {tracker_name_to_find} do NS: {e_remove}")
                    continue
                except Exception as e:
                    self.logger.warning(
                        f"Erro ao tentar conectar/pingar ao tracker {tracker_name_to_find} (URI {uri_obj_from_ns if uri_obj_from_ns else 'desconhecido'}): {e}")
                    continue
        else:
            self.logger.warning("Não foi possível conectar ao servidor de nomes durante a descoberta de tracker.")

        if tracker_uri_to_connect:
            self._connect_to_tracker(tracker_uri_to_connect, latest_epoch_found)
        else:
            if self.peer_id == "Peer1" and self.current_tracker_epoch == 0 and latest_epoch_found == -1:
                initial_epoch_for_peer1 = 1
                self.logger.info(
                    f"Nenhum tracker ativo encontrado. Como sou {self.peer_id} e não conheço nenhuma época, tentarei me tornar o tracker inicial da Época {initial_epoch_for_peer1}.")

                ns_proxy_check_peer1 = None
                can_become_tracker = True
                try:
                    ns_proxy_check_peer1 = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
                    # Verifica se já existe um tracker para a época 1
                    ns_proxy_check_peer1.lookup(f"{TRACKER_BASE_NAME}{initial_epoch_for_peer1}")
                    self.logger.info(
                        f"Um tracker para a Época {initial_epoch_for_peer1} já foi registrado por outro peer enquanto eu verificava. Iniciando eleição normal.")
                    can_become_tracker = False
                except Pyro5.errors.NamingError:
                    if not ns_proxy_check_peer1:
                        self.logger.warning(
                            f"Servidor de nomes inacessível ao tentar verificar {TRACKER_BASE_NAME}{initial_epoch_for_peer1} para Peer1. Não posso me tornar tracker.")
                        can_become_tracker = False
                except Exception as e_check:
                    self.logger.error(f"Erro ao verificar tracker para Peer1: {e_check}")
                    can_become_tracker = False

                if can_become_tracker:
                    self.logger.info(f"Assumindo como Tracker inicial da Época {initial_epoch_for_peer1}.")
                    self._become_tracker(initial_epoch_for_peer1)
                else:
                    self.initiate_election()
            else:
                self.logger.info("Nenhum tracker ativo encontrado. Iniciando eleição.")
                self.initiate_election()

    def _connect_to_tracker(self, tracker_uri, epoch):
        # Conecta ao tracker encontrado e registra arquivos
        tracker_uri_str = str(tracker_uri)
        try:
            if self.current_tracker_uri_str == tracker_uri_str and self.current_tracker_epoch == epoch:
                self.logger.debug(f"Já conectado ao tracker {tracker_uri_str} da época {epoch}.")
                if not self.is_tracker: self._start_tracker_timeout_detection()
                return

            self.logger.info(f"Tentando conectar ao Tracker {tracker_uri_str} (Época {epoch}).")
            new_tracker_proxy = Pyro5.api.Proxy(tracker_uri_str)
            new_tracker_proxy._pyroTimeout = 5
            new_tracker_proxy.ping()

            self.current_tracker_uri_str = tracker_uri_str
            self.current_tracker_proxy = new_tracker_proxy
            self.current_tracker_epoch = epoch
            self.is_tracker = (str(self.uri) == self.current_tracker_uri_str)

            if not self.is_tracker:
                self.logger.info(
                    f"Conectado ao Tracker_Epoca_{epoch} ({self.current_tracker_uri_str}). Registrando meus arquivos...")

                if hasattr(self, 'candidate_for_epoch_value') and \
                        self.candidate_for_epoch_value > 0 and \
                        self.candidate_for_epoch_value <= epoch:
                    self.logger.info(
                        f"Conectei ao tracker da época {epoch}. Cancelando minha candidatura pendente para época {self.candidate_for_epoch_value}.")
                    if self.election_vote_collection_timer and self.election_vote_collection_timer.is_alive():
                        self.election_vote_collection_timer.cancel()
                    self.candidate_for_epoch = 0
                    self.votes_received_for_epoch.pop(self.candidate_for_epoch_value, None)
                    self.candidate_for_epoch_value = 0

                # Registro inicial é sempre uma lista completa de arquivos
                response = self.current_tracker_proxy.register_files(
                    self.peer_id,
                    str(self.uri),
                    self.local_files,  # Envia a lista completa no primeiro registro
                    self.current_tracker_epoch,
                    is_incremental_update=False  # Primeiro registro não é incremental
                )
                if isinstance(response, dict) and response.get("status") == "epoch_too_low":
                    self.logger.warning(
                        f"Ao registrar, tracker {self.current_tracker_uri_str} informou que minha época ({self.current_tracker_epoch}) é baixa. Tracker real é {response.get('current_tracker_epoch')}. Descobrindo novamente.")
                    self._clear_current_tracker()
                    self.current_tracker_epoch = response.get('current_tracker_epoch', self.current_tracker_epoch)
                    self._discover_tracker()
                    return

                self._start_tracker_timeout_detection()
            else:
                self.logger.info(f"Eu sou o Tracker_Epoca_{epoch}.")
                self._stop_tracker_timeout_detection()
                self._start_sending_heartbeats()

        except Pyro5.errors.CommunicationError:
            self.logger.error(
                f"Falha de comunicação ao conectar/registrar com Tracker_Epoca_{epoch} ({tracker_uri_str}).")  # Alterado para tracker_uri_str
            self._clear_current_tracker()
            self.initiate_election()
        except Exception as e:
            self.logger.error(f"Erro ao conectar ao tracker {tracker_uri_str}: {e}")  # Alterado para tracker_uri_str
            self._clear_current_tracker()
            self.initiate_election()

    def _clear_current_tracker(self):
        # Limpa dados do tracker atual e cancela timers relacionados
        self.current_tracker_uri_str = None
        self.current_tracker_proxy = None
        if self.is_tracker:
            self._stop_sending_heartbeats()
        self.is_tracker = False
        self._stop_tracker_timeout_detection()

    def _handle_tracker_communication_error(self):
        # Trata falhas de comunicação com o tracker e inicia eleição
        tracker_uri_at_error = self.current_tracker_uri_str
        tracker_epoch_at_error = self.current_tracker_epoch
        self.logger.warning(
            f"Erro de comunicação com o tracker (URI: {tracker_uri_at_error}, Época: {tracker_epoch_at_error}). Considerando-o como falho.")

        if str(self.uri) == tracker_uri_at_error and self.is_tracker:
            self.logger.info("Eu era o tracker e detectei um problema. Parando atividades de tracker.")

        self._clear_current_tracker()
        self.current_tracker_epoch = tracker_epoch_at_error
        self.initiate_election()

    # Lógica de eleição
    def initiate_election(self):
        # Começa processo de eleição para escolher novo tracker

        # Se já existir um timer para coletar votos de uma eleição anterior, cancela-o.
        # Isso evita que timers antigos interfiram na nova eleição.
        if self.election_vote_collection_timer and self.election_vote_collection_timer.is_alive():
            self.election_vote_collection_timer.cancel()

        # Determina a maior época (epoch) que este peer conhece.
        # Inicializa com a época do tracker atual que o peer conhecia.
        max_known_epoch = self.current_tracker_epoch

        # Verifica se o peer já votou em alguma época e, se sim,
        # atualiza max_known_epoch caso encontre uma época maior no histórico de votos.
        if self.voted_in_epoch:
            max_known_epoch = max(max_known_epoch, max(self.voted_in_epoch.keys(), default=-1))

        # Verifica o histórico de épocas para as quais este peer já foi candidato.
        # Atualiza max_known_epoch se uma época de candidatura anterior for maior.
        # 'candidate_for_epoch_value_history' rastreia a maior época para a qual este peer tentou se candidatar.
        if hasattr(self, 'candidate_for_epoch_value_history') and self.candidate_for_epoch_value_history > 0:
            max_known_epoch = max(max_known_epoch, self.candidate_for_epoch_value_history)

        # A nova eleição ocorrerá para a próxima época após a maior época conhecida.
        new_election_epoch = max_known_epoch + 1

        # Se o peer já é um candidato ativo para esta nova época de eleição,
        # não faz nada e apenas aguarda os resultados da eleição em andamento.
        if self.candidate_for_epoch == 1 and self.candidate_for_epoch_value == new_election_epoch:
            self.logger.info(f"Já sou candidato para a época {new_election_epoch}. Aguardando resultado.")
            return

        # Define este peer como candidato para a nova época de eleição.
        self.candidate_for_epoch = 1  # Flag indicando que é um candidato.
        self.candidate_for_epoch_value = new_election_epoch # A época para a qual está se candidatando.
        # Atualiza o histórico da maior época para a qual este peer se candidatou.
        self.candidate_for_epoch_value_history = new_election_epoch

        self.logger.info(f"Iniciando eleição para Tracker_Epoca_{self.candidate_for_epoch_value}.")

        # Registra o voto próprio: o candidato automaticamente vota em si mesmo.
        # Inicializa o conjunto de votos recebidos para esta época, adicionando o próprio URI.
        self.votes_received_for_epoch[self.candidate_for_epoch_value] = {str(self.uri)}
        # Marca que este peer votou em si mesmo para esta época.
        self.voted_in_epoch[self.candidate_for_epoch_value] = str(self.uri)

        # Obtém uma lista dos URIs de outros peers na rede.
        other_peer_uris = self._get_other_peer_uris()

        # Caso especial: se não houver outros peers e o quórum necessário for 1 ou menos,
        # o peer tenta se eleger sozinho imediatamente.
        if not other_peer_uris and QUORUM <= 1:
            self.logger.info("Nenhum outro peer encontrado. Tentando me eleger sozinho (Quorum=1).")
            self._check_election_results() # Verifica imediatamente se pode se tornar tracker.
            return
        # Se não houver outros peers e o quórum for maior que 1,
        # o peer não pode se eleger sozinho e registra essa informação.
        elif not other_peer_uris:
            self.logger.info(f"Nenhum outro peer encontrado. Quorum é {QUORUM}, preciso de mais peers para me eleger.")
            # A eleição prosseguirá, mas provavelmente falhará se o QUORUM não for atingido.
            # O timer _check_election_results ainda será configurado.
            pass # Continua para configurar o timer de coleta de votos.

        # Informa quantos peers serão contatados para solicitar votos.
        self.logger.info(
            f"Solicitando votos de {len(other_peer_uris)} peers para época {self.candidate_for_epoch_value}.")

        # Envia solicitações de voto para todos os outros peers em threads separadas.
        # Isso permite que as solicitações sejam feitas em paralelo, sem bloquear o peer candidato.
        for peer_uri_str in other_peer_uris:
            try:
                # Cria uma nova thread para cada solicitação de voto.
                # A função _send_vote_request_to_peer será executada na nova thread.
                # args: URI do peer de quem solicitar o voto, e a época da eleição.
                threading.Thread(target=self._send_vote_request_to_peer,
                                 args=(peer_uri_str, self.candidate_for_epoch_value)).start()
            except Exception as e:
                self.logger.error(f"Erro ao criar thread para solicitar voto de {peer_uri_str} durante eleição: {e}")

        # Configura um timer para verificar os resultados da eleição após um certo tempo (ELECTION_REQUEST_TIMEOUT).
        # Cancela qualquer timer anterior que possa estar ativo.
        if self.election_vote_collection_timer and self.election_vote_collection_timer.is_alive():
            self.election_vote_collection_timer.cancel()
        # Cria um novo timer que chamará a função _check_election_results.
        self.election_vote_collection_timer = threading.Timer(ELECTION_REQUEST_TIMEOUT, self._check_election_results)
        self.election_vote_collection_timer.daemon = True # Permite que o programa saia mesmo se o timer estiver ativo.
        self.election_vote_collection_timer.start() # Inicia o timer.

    def _send_vote_request_to_peer(self, peer_uri_str,
                                   election_epoch_of_request):

        # Esta função é responsável por contatar outro peer e solicitar seu voto para uma eleição específica.

        local_peer_proxy = None  # Inicializa a variável do proxy do peer.
        try:
            # Cria um proxy para se comunicar com o peer especificado pelo URI.
            # O proxy permite chamar métodos remotos no objeto do outro peer.
            local_peer_proxy = Pyro5.api.Proxy(peer_uri_str)
            # Define um timeout curto para a chamada remota, para não bloquear por muito tempo se o peer não responder.
            local_peer_proxy._pyroTimeout = 2

            # Antes de solicitar o voto, verifica se este peer (o solicitante) ainda é um candidato ativo
            # para a época da eleição em questão.
            # Se a candidatura foi cancelada ou mudou para outra época, não prossegue com a solicitação.
            if self.candidate_for_epoch == 0 or self.candidate_for_epoch_value != election_epoch_of_request:
                self.logger.info(
                    f"Minha candidatura para época {election_epoch_of_request} foi cancelada/mudou. Não solicitando voto de {peer_uri_str}.")
                return  # Encerra a função se não for mais um candidato válido.

            # Loga a ação de solicitar o voto.
            self.logger.info(f"Solicitando voto de {peer_uri_str} para época {election_epoch_of_request}")
            # Chama o método remoto 'request_vote' no peer de destino.
            # Envia o URI do peer candidato (self.uri) e a época da eleição.
            vote_granted = local_peer_proxy.request_vote(str(self.uri), election_epoch_of_request)

            # Após receber a resposta, verifica novamente se a candidatura ainda é válida.
            # Isso é importante porque a resposta do voto pode demorar, e o estado da candidatura pode ter mudado nesse meio tempo.
            if self.candidate_for_epoch == 0 or self.candidate_for_epoch_value != election_epoch_of_request:
                self.logger.info(
                    f"Recebi resposta de voto de {peer_uri_str} para {election_epoch_of_request}, mas minha candidatura mudou/foi cancelada.")
                return  # Encerra se a candidatura não for mais válida.

            # Se o voto foi concedido pelo outro peer:
            if vote_granted:
                self.logger.info(f"Voto recebido de {peer_uri_str} para época {election_epoch_of_request}.")
                # Garante que existe uma entrada no dicionário de votos recebidos para esta época.
                if election_epoch_of_request not in self.votes_received_for_epoch:
                    self.votes_received_for_epoch[election_epoch_of_request] = set()
                # Adiciona o URI do peer que concedeu o voto ao conjunto de votos recebidos para esta eleição.
                self.votes_received_for_epoch[election_epoch_of_request].add(peer_uri_str)
            else:
                # Se o voto foi negado.
                self.logger.info(f"Voto negado por {peer_uri_str} para época {election_epoch_of_request}.")
        except Pyro5.errors.CommunicationError:
            # Captura erros de comunicação com o peer (ex: peer offline, rede instável).
            self.logger.warning(
                f"Falha ao solicitar voto de {peer_uri_str} para época {election_epoch_of_request} (CommunicationError).")
        except Exception as e:
            # Captura quaisquer outros erros que possam ocorrer durante a solicitação de voto.
            self.logger.error(f"Erro ao solicitar voto de {peer_uri_str} para época {election_epoch_of_request}: {e}")

    @Pyro5.api.expose
    def request_vote(self, candidate_uri_str, election_epoch):
        # Este método é chamado por um peer candidato para solicitar o voto deste peer em uma eleição.
        # candidate_uri_str: O URI do peer que está se candidatando.
        # election_epoch: A época (número sequencial) da eleição para a qual o voto está sendo solicitado.

        self.logger.info(f"Pedido de voto recebido de {candidate_uri_str} para Tracker_Epoca_{election_epoch}.")
        # Log detalhado do estado atual do peer para ajudar na depuração da lógica de votação.
        self.logger.info(
            f"Meu estado: current_tracker_epoch={self.current_tracker_epoch}, voted_in_epoch[{election_epoch}]={self.voted_in_epoch.get(election_epoch)}, current_tracker_uri={self.current_tracker_uri_str}, sou_candidato_para_epoca={self.candidate_for_epoch_value if self.candidate_for_epoch else 'Nao'}")

        # --- REGRA 1: Não votar em eleições para épocas passadas se já conheço um tracker ativo ---
        # Se a época da eleição solicitada é anterior à época do tracker que este peer considera ativo,
        # o voto é negado. Isso evita voltar para um estado anterior da rede.
        if election_epoch < self.current_tracker_epoch and self.current_tracker_uri_str is not None:
            self.logger.info(
                f"Voto negado (Regra 1): época da eleição ({election_epoch}) é menor que a do tracker atual ({self.current_tracker_epoch}) que considero ativo.")
            return False  # Nega o voto.

        # --- REGRA 2: Não votar se a eleição é para a mesma época do tracker ativo e o candidato é diferente ---
        # Se a eleição é para a mesma época do tracker que este peer já considera ativo,
        # e o candidato que está pedindo voto não é esse tracker ativo, o voto é negado.
        # Isso dá preferência ao tracker já estabelecido para a época corrente.
        if election_epoch == self.current_tracker_epoch and \
                self.current_tracker_uri_str is not None and \
                self.current_tracker_uri_str != candidate_uri_str:
            self.logger.info(
                f"Voto negado (Regra 2): época da eleição ({election_epoch}) é a mesma do tracker atual ({self.current_tracker_uri_str}) que considero ativo, e candidato é outro.")
            return False  # Nega o voto.

        # --- REGRA 3: Lógica para quando já existe um voto registrado para a época da eleição ---
        # Verifica se este peer já votou em alguém para a 'election_epoch'.
        if election_epoch in self.voted_in_epoch:
            current_voted_candidate_in_epoch = self.voted_in_epoch[election_epoch]  # Pega o URI em quem já votou.

            # REGRA 3a: Se já votou no candidato atual, confirma o voto.
            if current_voted_candidate_in_epoch == candidate_uri_str:
                self.logger.info(f"Já votei em {candidate_uri_str} para a época {election_epoch}. Confirmando voto.")
                return True  # Concede o voto (ou confirma o voto anterior).

            # REGRA 3b (Mudança de voto): Se este peer tinha votado em si mesmo, mas o novo candidato tem um URI "menor" (critério de desempate),
            # o peer muda seu voto para o novo candidato. URIs menores têm preferência em caso de empate na época.
            if current_voted_candidate_in_epoch == str(self.uri) and \
                    candidate_uri_str != str(self.uri) and \
                    candidate_uri_str < str(self.uri):  # Compara lexicograficamente os URIs.
                self.logger.info(
                    f"Eu votei em mim ({str(self.uri)}) para época {election_epoch}, mas {candidate_uri_str} tem URI menor. Mudando meu voto para {candidate_uri_str}.")
                self.voted_in_epoch[election_epoch] = candidate_uri_str  # Atualiza o voto.

                # Se este peer era um candidato ativo para esta época, ele cancela sua própria candidatura,
                # pois agora está votando em outro.
                if self.candidate_for_epoch == 1 and self.candidate_for_epoch_value == election_epoch:
                    self.logger.info(
                        f"Cancelando minha candidatura ativa para época {election_epoch} porque votei em {candidate_uri_str} (URI menor).")
                    if self.election_vote_collection_timer and self.election_vote_collection_timer.is_alive():
                        self.election_vote_collection_timer.cancel()  # Para o timer de coleta de votos da sua candidatura.
                    self.candidate_for_epoch = 0  # Deixa de ser candidato.
                    self.votes_received_for_epoch.pop(election_epoch, None)  # Remove os votos que recebeu.
                self._stop_tracker_timeout_detection()  # Para o timer de detecção de falha do tracker, pois uma eleição está em progresso.
                return True  # Concede o voto ao novo candidato.
            else:
                # REGRA 3c: Se já votou em outro candidato (que não ele mesmo), ou se votou em si mesmo mas o novo candidato não tem URI menor,
                # mantém o voto original e nega o voto ao solicitante atual.
                self.logger.info(
                    f"Voto negado (Regra 3b): já votei em {current_voted_candidate_in_epoch} para a época {election_epoch} e não mudarei (ou meu URI é menor/igual, ou já votei em terceiro).")
                return False  # Nega o voto.

        # --- REGRA 4: Primeiro voto para esta época ---
        # Se nenhuma das condições anteriores foi atendida, significa que este peer ainda não votou
        # para 'election_epoch'. Portanto, concede o voto ao solicitante.
        self.voted_in_epoch[election_epoch] = candidate_uri_str  # Registra o voto.
        self.logger.info(
            f"Voto concedido para {candidate_uri_str} para Tracker_Epoca_{election_epoch} (primeiro voto nesta época).")

        # Para o timer de detecção de falha do tracker, pois uma eleição está em progresso e um voto foi dado.
        # Isso evita que o peer inicie uma nova eleição prematuramente.
        self._stop_tracker_timeout_detection()

        # Se este peer era um candidato para uma época igual ou anterior à 'election_epoch'
        # e agora está votando em outro candidato (diferente de si mesmo),
        # ele deve cancelar sua própria candidatura.
        if self.candidate_for_epoch == 1 and \
                self.candidate_for_epoch_value > 0 and \
                self.candidate_for_epoch_value <= election_epoch and \
                str(self.uri) != candidate_uri_str:  # Verifica se não está votando em si mesmo.
            self.logger.info(
                f"Eu era candidato para época {self.candidate_for_epoch_value}, mas votei em {candidate_uri_str} para época {election_epoch}. Cancelando minha candidatura.")
            if self.election_vote_collection_timer and self.election_vote_collection_timer.is_alive():
                self.election_vote_collection_timer.cancel()  # Para o timer de coleta de votos.
            self.candidate_for_epoch = 0  # Deixa de ser candidato.
            # Remove os votos que possa ter recebido para sua candidatura cancelada.
            self.votes_received_for_epoch.pop(self.candidate_for_epoch_value, None)

        return True  # Concede o voto.

    def _check_election_results(self):
        # Este método é chamado para verificar o resultado de uma eleição na qual este peer foi um candidato.
        # Geralmente é acionado após o término do timer de coleta de votos (self.election_vote_collection_timer).

        # Obtém a época da eleição para a qual este peer foi candidato.
        # self.candidate_for_epoch_value armazena a época se self.candidate_for_epoch for 1.
        election_epoch_being_checked = self.candidate_for_epoch_value

        # Se o peer não é mais um candidato ativo (candidate_for_epoch == 0) ou
        # se a época da candidatura é inválida (0), não há o que verificar.
        if self.candidate_for_epoch == 0 or election_epoch_being_checked == 0:
            self.logger.debug(
                f"Verificação de resultados de eleição, mas não sou candidato ativo ou a época da candidatura é 0 (época: {election_epoch_being_checked}).")
            return  # Encerra a função.

        # Verifica se há algum registro de votos recebidos para a época da candidatura.
        # Se não houver entrada no dicionário self.votes_received_for_epoch, significa que nenhum voto foi computado.
        if election_epoch_being_checked not in self.votes_received_for_epoch:
            self.logger.info(f"Nenhum voto registrado para minha candidatura da época {election_epoch_being_checked}.")
            self.candidate_for_epoch = 0  # Marca que não é mais candidato.
            return  # Encerra a função.

        # Calcula o número de votos recebidos para a candidatura.
        # self.votes_received_for_epoch[election_epoch_being_checked] é um set contendo os URIs dos peers que votaram.
        # O .get() com um set vazio como default é uma segurança, embora a verificação anterior já cubra o caso de não existência da chave.
        num_votes = len(self.votes_received_for_epoch.get(election_epoch_being_checked, set()))
        self.logger.info(
            f"Eleição para época {election_epoch_being_checked}: {num_votes} votos recebidos. Quórum necessário: {QUORUM}.")

        # Compara o número de votos recebidos com o quórum necessário para vencer a eleição.
        if num_votes >= QUORUM:
            # Se o número de votos é maior ou igual ao quórum, o peer foi eleito.
            self.logger.info(f"Quórum atingido! Eleito como Tracker_Epoca_{election_epoch_being_checked}.")
            # Chama o método para se tornar o tracker para a época em que foi eleito.
            self._become_tracker(election_epoch_being_checked)
        else:
            # Se o quórum não foi atingido, a eleição falhou para esta tentativa.
            self.logger.info(
                f"Quórum não atingido para época {election_epoch_being_checked}. Eleição falhou para esta tentativa.")
            # Remove o registro de votos para esta época, já que a tentativa falhou.
            # Isso limpa o estado para futuras eleições ou candidaturas.
            self.votes_received_for_epoch.pop(election_epoch_being_checked, None)

        # Independentemente de ter vencido ou perdido, o peer não é mais considerado um candidato ativo
        # para esta eleição específica após a verificação dos resultados.
        # Se ele se tornou tracker, o estado de candidatura é resetado em _become_tracker.
        # Se perdeu, ele também não é mais candidato para *esta* rodada/época.
        self.candidate_for_epoch = 0


    def _become_tracker(self, epoch):
        # Assume o papel de tracker e registra no nameserver
        if self.is_tracker and self.current_tracker_epoch == epoch:
            self.logger.info(f"Já sou o tracker para a época {epoch}.")
            return

        self.logger.info(f"Tornando-me Tracker_Epoca_{epoch}.")
        ns_proxy_local_become = None
        try:
            ns_proxy_local_become = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
        except Pyro5.errors.NamingError:
            self.logger.error(
                f"Falha ao conectar ao servidor de nomes em _become_tracker. Não é possível registrar como tracker.")
            self.is_tracker = False
            return

        self.is_tracker = True
        self.current_tracker_epoch = epoch
        self.current_tracker_uri_str = str(self.uri)
        self.current_tracker_proxy = self

        self.candidate_for_epoch = 0
        self.candidate_for_epoch_value = 0
        self.votes_received_for_epoch = {}

        tracker_name = f"{TRACKER_BASE_NAME}{epoch}"
        try:
            try:
                existing_uri = ns_proxy_local_become.lookup(tracker_name)
                if existing_uri != self.uri:
                    self.logger.warning(
                        f"Já existe um registro para {tracker_name} com URI {existing_uri} (diferente do meu). Tentando remover e registrar o meu.")
                    ns_proxy_local_become.remove(tracker_name)
                elif str(existing_uri) == str(self.uri):
                    self.logger.info(f"Já estou registrado como {tracker_name}. Apenas confirmando.")

            except Pyro5.errors.NamingError:
                pass

            ns_proxy_local_become.register(tracker_name, self.uri)
            self.logger.info(f"Registrado no servidor de nomes como {tracker_name} (URI: {self.uri}).")
        except Exception as e:
            self.logger.error(f"Falha ao registrar como {tracker_name} no servidor de nomes: {e}")
            self._step_down_as_tracker()
            return

        self.file_index = {}
        # Ao se tornar tracker, registra seus próprios arquivos com uma atualização completa.
        self._update_tracker_index_for_peer(self.peer_id, str(self.uri), self.local_files, is_incremental=False)

        self._stop_tracker_timeout_detection()
        self._start_sending_heartbeats()

    def _step_down_as_tracker(self):
        # Renuncia ao tracker e remove registro do servidor de nomes
        if not self.is_tracker:
            return

        epoch_i_was_tracker = self.current_tracker_epoch
        self.logger.info(f"Renunciando ao posto de Tracker_Epoca_{epoch_i_was_tracker}.")
        self._stop_sending_heartbeats()

        tracker_name = f"{TRACKER_BASE_NAME}{epoch_i_was_tracker}"
        ns_proxy_local_stepdown = None
        try:
            ns_proxy_local_stepdown = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
            registered_uri = ns_proxy_local_stepdown.lookup(tracker_name)
            if str(registered_uri) == str(self.uri):
                ns_proxy_local_stepdown.remove(tracker_name)
                self.logger.info(f"Removido {tracker_name} (meu registro) do servidor de nomes.")
            else:
                self.logger.info(f"Não removi {tracker_name} do NS, pois o URI ({registered_uri}) não é o meu.")
        except Pyro5.errors.NamingError:
            self.logger.info(
                f"{tracker_name} não encontrado no servidor de nomes para remoção (pode já ter sido substituído).")
        except Pyro5.errors.CommunicationError:
            self.logger.warning(
                f"Servidor de nomes inacessível em _step_down_as_tracker ao tentar remover {tracker_name}.")
        except Exception as e:
            self.logger.error(f"Erro ao tentar remover/verificar {tracker_name} do NS: {e}")

        self._clear_current_tracker()
        self.current_tracker_epoch = epoch_i_was_tracker

        self.logger.info("Após renunciar, vou tentar descobrir um novo tracker ou participar de eleição.")
        self._discover_tracker()

    # Heartbeat e detecção de falha
    def _start_tracker_timeout_detection(self):
        # Agenda timer para monitorar falhas do tracker
        if self.is_tracker:
            self._stop_tracker_timeout_detection()
            return

        self._stop_tracker_timeout_detection()

        if self.current_tracker_proxy and self.current_tracker_uri_str:
            timeout = random.uniform(TRACKER_DETECTION_TIMEOUT_MIN, TRACKER_DETECTION_TIMEOUT_MAX)
            self.tracker_timeout_timer = threading.Timer(timeout, self._handle_tracker_timeout)
            self.tracker_timeout_timer.daemon = True
            self.tracker_timeout_timer.start()
            self.logger.info(
                f"Timer de detecção de falha do tracker iniciado ({timeout:.2f}s) para {self.current_tracker_uri_str} (Época {self.current_tracker_epoch}).")
        else:
            self.logger.debug("Não iniciando timer de detecção de falha: nenhum tracker atual definido.")

    def _stop_tracker_timeout_detection(self):
        # Cancela timer de detecção de falha
        if self.tracker_timeout_timer and self.tracker_timeout_timer.is_alive():
            self.tracker_timeout_timer.cancel()
            self.logger.debug("Timer de detecção de falha do tracker parado.")

    def _handle_tracker_timeout(self):
        # Executa ao expirar timer de falha do tracker e inicia eleição
        if self.is_tracker: return

        if self.current_tracker_proxy is None or self.current_tracker_uri_str is None:
            self.logger.info(
                "Timeout do tracker, mas já não há tracker atual definido (pode ter mudado). Ignorando este timeout.")
            return

        tracker_uri_timed_out = self.current_tracker_uri_str
        epoch_timed_out = self.current_tracker_epoch
        self.logger.warning(
            f"Timeout: Tracker {tracker_uri_timed_out} (Epoca {epoch_timed_out}) não respondeu (ou não enviou heartbeat válido).")

        self._clear_current_tracker()
        self.current_tracker_epoch = epoch_timed_out

        self.initiate_election()

    def _start_sending_heartbeats(self):
        # Começa a enviar heartbeats periodicamente se eu for tracker
        if not self.is_tracker:
            return

        self._stop_sending_heartbeats()

        def heartbeat_wrapper():
            if self.is_tracker:
                self._send_heartbeat_and_reschedule()
            else:
                self.logger.info("Não sou mais tracker, parando envio de heartbeats agendado.")
                self._heartbeat_logging_once = False

        self.heartbeat_send_timer = threading.Timer(HEARTBEAT_INTERVAL, heartbeat_wrapper)
        self.heartbeat_send_timer.daemon = True
        self.heartbeat_send_timer.start()

        if not hasattr(self, '_heartbeat_logging_once') or not self._heartbeat_logging_once:
            self.logger.info(f"Tracker: Envio de heartbeats iniciado para época {self.current_tracker_epoch}.")
            self._heartbeat_logging_once = True

    def _stop_sending_heartbeats(self):
        # Interrompe envio de heartbeats
        if self.heartbeat_send_timer and self.heartbeat_send_timer.is_alive():
            self.heartbeat_send_timer.cancel()
            self.logger.info("Tracker: Envio de heartbeats parado.")
            self._heartbeat_logging_once = False

    def _send_heartbeat_and_reschedule(self):
        # Envia heartbeat e já prepara o próximo envio
        if not self.is_tracker:
            self.logger.info("Tentativa de enviar heartbeat, mas não sou mais tracker.")
            return

        self.logger.debug(f"Tracker: Enviando heartbeat da época {self.current_tracker_epoch}.")
        other_peer_uris = self._get_other_peer_uris()

        for peer_uri_str in other_peer_uris:
            try:
                threading.Thread(target=self._safe_send_heartbeat_to_one_peer,
                                 args=(peer_uri_str, str(self.uri),
                                       self.current_tracker_epoch)).start()  # Removido peer_proxy
            except Exception as e:
                self.logger.warning(f"Tracker: Falha ao criar thread para enviar heartbeat para {peer_uri_str}: {e}")

        if self.is_tracker:
            self._start_sending_heartbeats()

    def _safe_send_heartbeat_to_one_peer(self, target_peer_uri_str, tracker_uri_str,
                                         tracker_epoch):  # Removido peer_proxy
        # Tenta enviar heartbeat a um peer e captura falhas sem interromper o tracker
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
        # Processa heartbeat recebido e decide se mantenho ou renuncio
        self.logger.debug(
            f"Heartbeat recebido de {incoming_tracker_uri_str} (Epoca {incoming_tracker_epoch}). Meu tracker: {self.current_tracker_uri_str} (Epoca {self.current_tracker_epoch}). Sou tracker: {self.is_tracker}")

        if self.is_tracker:
            if incoming_tracker_uri_str == str(self.uri):
                return
            if incoming_tracker_epoch > self.current_tracker_epoch:
                self.logger.warning(
                    f"Sou Tracker (Época {self.current_tracker_epoch}), mas recebi heartbeat de {incoming_tracker_uri_str} (Época {incoming_tracker_epoch}). Época dele é MAIOR. Renunciando.")
                self._step_down_as_tracker()
                return
            elif incoming_tracker_epoch == self.current_tracker_epoch:
                if incoming_tracker_uri_str < str(self.uri):
                    self.logger.warning(
                        f"Sou Tracker (Época {self.current_tracker_epoch}), mas recebi heartbeat de {incoming_tracker_uri_str} (MESMA época) com URI MENOR. Renunciando.")
                    self._step_down_as_tracker()
                    return
                else:
                    self.logger.info(
                        f"Sou Tracker (Época {self.current_tracker_epoch}), recebi heartbeat de {incoming_tracker_uri_str} (MESMA época) com URI MAIOR/IGUAL. Ignorando o dele.")
                    return
            else:
                self.logger.info(
                    f"Sou Tracker (Época {self.current_tracker_epoch}), recebi heartbeat de um tracker antigo/inferior ({incoming_tracker_uri_str}, Época {incoming_tracker_epoch}). Ignorando.")
                return

        if incoming_tracker_epoch > self.current_tracker_epoch:
            self.logger.info(
                f"Novo tracker ou tracker com época superior detectado (Época {incoming_tracker_epoch} > {self.current_tracker_epoch}). Conectando a {incoming_tracker_uri_str}.")
            self._stop_tracker_timeout_detection()
            self.voted_in_epoch = {e: c for e, c in self.voted_in_epoch.items() if e >= incoming_tracker_epoch}
            self._connect_to_tracker(Pyro5.api.URI(incoming_tracker_uri_str), incoming_tracker_epoch)

        elif incoming_tracker_epoch == self.current_tracker_epoch:
            if self.current_tracker_uri_str is None:
                self.logger.info(
                    f"Recebi heartbeat de tracker {incoming_tracker_uri_str} para época {incoming_tracker_epoch} (eu não tinha tracker para esta época). Conectando.")
                self._connect_to_tracker(Pyro5.api.URI(incoming_tracker_uri_str), incoming_tracker_epoch)
            elif self.current_tracker_uri_str == incoming_tracker_uri_str:
                self.logger.debug(
                    f"Heartbeat válido do tracker atual {self.current_tracker_uri_str}. Reiniciando timer de timeout.")
                self._start_tracker_timeout_detection()
            else:
                if incoming_tracker_uri_str < self.current_tracker_uri_str:
                    self.logger.warning(
                        f"Heartbeat de tracker alternativo {incoming_tracker_uri_str} (época {incoming_tracker_epoch}) com URI MENOR que meu tracker atual {self.current_tracker_uri_str}. Mudando.")
                    self._stop_tracker_timeout_detection()
                    self._connect_to_tracker(Pyro5.api.URI(incoming_tracker_uri_str), incoming_tracker_epoch)
                else:
                    self.logger.info(
                        f"Heartbeat de tracker alternativo {incoming_tracker_uri_str} (época {incoming_tracker_epoch}), mas meu tracker atual {self.current_tracker_uri_str} tem URI menor/igual. Mantendo o atual e reiniciando timer.")
                    self._start_tracker_timeout_detection()
        else:
            self.logger.debug(
                f"Heartbeat de tracker antigo/inferior ({incoming_tracker_uri_str}, Época {incoming_tracker_epoch}) ignorado.")

    # --- Funcionalidades do Tracker (quando self.is_tracker == True) ---
    @Pyro5.api.expose
    def register_files(self, peer_id_req, peer_uri_str_req, file_list_req, peer_tracker_epoch_view_req,
                       is_incremental_update=False):  # Adicionado is_incremental_update
        """Chamado por peers para registrar/atualizar seus arquivos no tracker."""
        if not self.is_tracker:
            self.logger.warning(
                f"Chamada para register_files ({peer_id_req}) recebida, mas não sou o tracker. Sou {self.peer_id} (época {self.current_tracker_epoch}). Tracker conhecido: {self.current_tracker_uri_str}")
            return {"status": "not_tracker",
                    "known_tracker_uri": self.current_tracker_uri_str,
                    "known_tracker_epoch": self.current_tracker_epoch}

        if peer_tracker_epoch_view_req < self.current_tracker_epoch:
            self.logger.warning(
                f"Tracker: Peer {peer_id_req} (URI {peer_uri_str_req}) tentou registrar com época antiga ({peer_tracker_epoch_view_req} vs minha {self.current_tracker_epoch}). Instruindo a atualizar.")
            return {"status": "epoch_too_low", "current_tracker_epoch": self.current_tracker_epoch}

        log_action = "adicionando incrementalmente" if is_incremental_update else "registrando/atualizando (completo)"
        self.logger.info(
            f"Tracker: {peer_id_req} ({peer_uri_str_req}) {log_action} arquivos (peer viu época {peer_tracker_epoch_view_req}): {file_list_req}")

        self._update_tracker_index_for_peer(peer_id_req, peer_uri_str_req, file_list_req,
                                            is_incremental=is_incremental_update)
        return {"status": "ok", "registered_at_epoch": self.current_tracker_epoch}

    def _update_tracker_index_for_peer(self, peer_id_to_update, peer_uri_to_update, new_file_list,
                                       is_incremental=False):
        """Lógica interna para atualizar o índice de arquivos para um peer específico."""
        if not is_incremental:
            # Lógica de atualização completa (remove todas as entradas antigas para este peer)
            self.logger.debug(f"Tracker: Executando atualização COMPLETA do índice para {peer_id_to_update}.")
            for filename in list(self.file_index.keys()):  # Itera sobre uma cópia das chaves
                current_holders = self.file_index.get(filename, set())
                # Filtra para remover o peer_id_to_update, mantendo outros
                updated_holders = {(pid, puri) for pid, puri in current_holders if pid != peer_id_to_update}

                if not updated_holders:  # Se o set ficar vazio
                    if filename in self.file_index:  # Verifica se a chave ainda existe antes de deletar
                        del self.file_index[filename]
                elif len(updated_holders) < len(current_holders):  # Se algo foi removido
                    self.file_index[filename] = updated_holders

            # Adiciona os novos arquivos da new_file_list (que é a lista completa neste caso)
            for filename in new_file_list:
                if filename not in self.file_index:
                    self.file_index[filename] = set()
                self.file_index[filename].add((peer_id_to_update, peer_uri_to_update))
        else:
            # Lógica de atualização incremental (APENAS adiciona os novos arquivos)
            self.logger.debug(
                f"Tracker: Executando atualização INCREMENTAL do índice para {peer_id_to_update} com arquivos: {new_file_list}.")
            if not new_file_list:  # Se a lista de novos arquivos estiver vazia, não faz nada.
                self.logger.debug(
                    f"Tracker: Nenhum arquivo novo para adicionar incrementalmente para {peer_id_to_update}.")
                return

            for filename in new_file_list:  # new_file_list contém apenas os arquivos a serem ADICIONADOS
                if filename not in self.file_index:
                    self.file_index[filename] = set()
                # Adiciona o peer ao arquivo, mesmo que já exista (o set cuida da duplicidade)
                self.file_index[filename].add((peer_id_to_update, peer_uri_to_update))
                self.logger.debug(f"Tracker: Adicionado/confirmado {filename} para {peer_id_to_update}.")

        self.logger.info(f"Tracker: Índice atualizado para {peer_id_to_update}. Índice agora: {self.file_index}")

    @Pyro5.api.expose
    def query_file(self, filename_req, asking_peer_epoch_view_req):
        """Chamado por peers para perguntar quem tem um arquivo."""
        if not self.is_tracker:
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
            return None  # Ou levantar uma exceção específica

        file_path = os.path.join(self.shared_folder, filename)
        try:
            with open(file_path, 'rb') as f:
                f.seek(chunk_offset)
                data = f.read(chunk_size)
                self.logger.debug(f"Enviando chunk de '{filename}', offset {chunk_offset}, size {len(data)}")
                return data
        except FileNotFoundError:
            self.logger.error(
                f"Arquivo '{filename}' não encontrado no caminho {file_path} ao tentar ler para download.")
            self.local_files = self._scan_local_files()  # Re-sincroniza se o arquivo sumiu
            self.update_local_files_and_notify_tracker()  # Notifica o tracker da mudança
            return None
        except Exception as e:
            self.logger.error(f"Erro ao ler arquivo '{filename}' para download: {e}")
            return None

    @Pyro5.api.expose
    def get_file_size(self, filename):
        """Retorna o tamanho de um arquivo local."""
        if filename not in self.local_files:
            self.logger.warning(f"Pedido de tamanho para arquivo '{filename}' que não possuo.")
            return -1

        file_path = os.path.join(self.shared_folder, filename)
        try:
            return os.path.getsize(file_path)
        except FileNotFoundError:
            self.logger.error(f"Arquivo '{filename}' não encontrado no caminho {file_path} ao tentar obter tamanho.")
            self.local_files = self._scan_local_files()  # Re-sincroniza
            self.update_local_files_and_notify_tracker()
            return -1
        except Exception as e:
            self.logger.error(f"Erro ao obter tamanho do arquivo {filename}: {e}")
            return -1

    def _get_other_peer_uris(self):
        """Busca URIs de outros peers no servidor de nomes, excluindo o próprio URI."""
        try:
            if not self.ns_proxy:  # Cria proxy se não existir
                self.ns_proxy = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
            peers_map = self.ns_proxy.list(prefix=PEER_NAME_PREFIX)
            return [uri for name, uri in peers_map.items() if uri != str(self.uri)]
        except Pyro5.errors.NamingError:
            self.logger.error(f"Servidor de nomes não encontrado ao listar outros peers.")
            self.ns_proxy = None  # Reseta o proxy para tentar reconectar depois
            return []
        except Exception as e:
            self.logger.error(f"Erro ao listar outros peers no servidor de nomes: {e}")
            return []

    # --- Interface de Usuário (CLI) ---
    def _handle_tracker_response_for_cli(self, response, operation_name="operação"):
        """Função auxiliar para tratar respostas comuns do tracker na CLI."""
        if not response:
            self.logger.error(f"Nenhuma resposta do tracker para {operation_name}.")
            return None

        status = response.get("status")
        if status == "epoch_too_low":
            new_epoch = response.get('current_tracker_epoch', self.current_tracker_epoch + 1)
            self.logger.warning(
                f"Minha visão da época do tracker ({self.current_tracker_epoch}) está desatualizada para {operation_name}. Tracker atual é época {new_epoch}. Tentando reconectar/descobrir...")
            self.current_tracker_epoch = new_epoch - 1
            self._discover_tracker()
            return None
        elif status == "not_tracker":
            self.logger.warning(
                f"O peer contatado não é o tracker para {operation_name}. Tracker conhecido por ele: {response.get('known_tracker_uri')} (Época {response.get('known_tracker_epoch')}). Tentando descobrir o tracker correto...")
            if response.get('known_tracker_epoch', -1) > self.current_tracker_epoch:
                self.current_tracker_epoch = response.get('known_tracker_epoch')
                self.current_tracker_uri_str = response.get('known_tracker_uri')
            self._discover_tracker()
            return None
        elif status != "ok":
            self.logger.error(f"Erro na {operation_name} com o tracker: {response}")
            return None
        return response

    def cli_search_file(self):
        filename = input("Digite o nome do arquivo para buscar: ")
        if not self.current_tracker_uri_str and not self.is_tracker:
            self.logger.info("Nenhum tracker ativo conhecido. Tentando descobrir...")
            self._discover_tracker()
            if not self.current_tracker_uri_str and not self.is_tracker:
                self.logger.info("Ainda não há tracker ativo após nova tentativa de descoberta.")
                return

        raw_response = None
        if self.is_tracker:
            self.logger.info(f"Consultando meu próprio índice (sou o tracker) por '{filename}'...")
            raw_response = self.query_file(filename, self.current_tracker_epoch)
        elif self.current_tracker_uri_str:
            try:
                tracker_proxy_local = Pyro5.api.Proxy(self.current_tracker_uri_str)
                tracker_proxy_local._pyroTimeout = 5
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
        else:
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
                else:  # Se 's' ou número inválido, pega o primeiro da lista
                    self.logger.info("Opção inválida ou 's', baixando do primeiro peer da lista.")

                chosen_peer_id, chosen_peer_uri_str = holders[target_peer_index]
                if str(self.uri) == chosen_peer_uri_str:
                    self.logger.info("Este peer já possui o arquivo localmente.")
                    # Verifica se o arquivo está na pasta de download ou na de compartilhamento
                    download_folder_check = os.path.join(os.getcwd(), "p2p_download_folders", self.peer_id)
                    if os.path.exists(os.path.join(download_folder_check, filename)):
                        self.logger.info(f"Arquivo '{filename}' já existe em {download_folder_check}")
                    elif os.path.exists(os.path.join(self.shared_folder, filename)):
                        self.logger.info(f"Arquivo '{filename}' já existe em {self.shared_folder}")
                    else:
                        self.logger.warning(
                            f"Arquivo '{filename}' reportado como local, mas não encontrado. Tentando baixar novamente se possível.")
                        # Poderia tentar baixar de outro peer aqui se a lógica fosse mais complexa
                    return

                download_folder = os.path.join(os.getcwd(), "p2p_download_folders", self.peer_id)
                os.makedirs(download_folder, exist_ok=True)
                self._download_file_from_peer(filename, chosen_peer_uri_str, download_folder)
        else:
            self.logger.info(f"Arquivo '{filename}' não encontrado na rede (segundo o tracker).")

    def _download_file_from_peer(self, filename, target_peer_uri_str, download_folder):
        """Baixa um arquivo de outro peer em chunks."""
        save_path = os.path.join(download_folder, filename)
        if os.path.exists(save_path):
            self.logger.info(f"Arquivo '{filename}' já existe em {save_path}. Download cancelado.")
            # Opcional: verificar hash ou tamanho para decidir se baixa novamente
            return

        try:
            target_peer_proxy = Pyro5.api.Proxy(target_peer_uri_str)
            target_peer_proxy._pyroTimeout = 10

            total_size = target_peer_proxy.get_file_size(filename)
            if total_size == -1:
                self.logger.error(
                    f"Arquivo '{filename}' não encontrado ou erro ao obter tamanho no peer de origem {target_peer_uri_str}.")
                return
            if total_size == 0:
                self.logger.info(f"Arquivo '{filename}' está vazio. Criando arquivo vazio localmente.")
                open(save_path, 'wb').close()
                print("\nDownload concluído (arquivo vazio)!")
                # Após o download, atualiza os arquivos locais e notifica o tracker
                return

            self.logger.info(f"Iniciando download de '{filename}' ({total_size} bytes) de {target_peer_uri_str}...")

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
                        else:  # Download completo, mas último chunk foi None (improvável se total_size > 0)
                            break

                    f.write(chunk_data)
                    bytes_downloaded += len(chunk_data)
                    progress = (bytes_downloaded / total_size) * 100 if total_size > 0 else 100
                    print(f"\rBaixando '{filename}': {bytes_downloaded}/{total_size} bytes ({progress:.2f}%)", end="")
            print("\nDownload concluído!")
            self.logger.info(f"Arquivo '{filename}' baixado para {save_path}.")
            # Se os arquivos baixados devem ser compartilhados, eles precisam ser movidos para self.shared_folder
            # e então self.update_local_files_and_notify_tracker() chamado.
            # Exemplo:
            # shutil.move(save_path, os.path.join(self.shared_folder, filename))
            # self.update_local_files_and_notify_tracker()

        except Pyro5.errors.CommunicationError:
            self.logger.error(f"Falha de comunicação com {target_peer_uri_str} durante o download.")
            if os.path.exists(save_path): os.remove(save_path)
        except Exception as e:
            self.logger.error(f"Erro ao baixar arquivo '{filename}' de {target_peer_uri_str}: {e}")
            if os.path.exists(save_path): os.remove(save_path)

    def cli_list_my_files(self):
        # Garante que a lista local_files está atualizada antes de listar
        self.local_files = self._scan_local_files()
        self.logger.info(f"Meus arquivos compartilhados ({len(self.local_files)}):")
        if not self.local_files:
            print("  (Nenhum arquivo local compartilhado)")
        for f_name in self.local_files:
            print(f"  - {f_name}")

    def cli_list_network_files(self):
        if not self.current_tracker_uri_str and not self.is_tracker:
            self.logger.info("Nenhum tracker ativo conhecido. Tentando descobrir...")
            self._discover_tracker()
            if not self.current_tracker_uri_str and not self.is_tracker:
                self.logger.info("Ainda não há tracker ativo após nova tentativa de descoberta.")
                return

        raw_response = None
        if self.is_tracker:
            self.logger.info("Consultando meu próprio índice (sou o tracker) por todos os arquivos...")
            raw_response = self.get_all_indexed_files(self.current_tracker_epoch)
        elif self.current_tracker_uri_str:
            try:
                tracker_proxy_local = Pyro5.api.Proxy(self.current_tracker_uri_str)
                tracker_proxy_local._pyroTimeout = 5
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
        else:
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
        self.update_local_files_and_notify_tracker()  # Esta função já atualiza self.local_files
        self.cli_list_my_files()  # Apenas lista o estado atual de self.local_files

    def cli_status(self):
        # Atualiza a lista de arquivos locais antes de exibir o status
        self.local_files = self._scan_local_files()

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
        else:
            status_msg += f"\nTracker Atual URI: {self.current_tracker_uri_str if self.current_tracker_uri_str else 'Nenhum conhecido'}"
            status_msg += f"\nTracker Atual Época (Conhecida): {self.current_tracker_epoch if self.current_tracker_uri_str else 'N/A'}"

        status_msg += f"\nMeus Arquivos Locais ({len(self.local_files)}): {self.local_files if self.local_files else 'Nenhum'}"

        voted_info_list = []
        for ep, cand_uri in self.voted_in_epoch.items():
            try:
                cand_peer_id_part = cand_uri.split('@')[0].replace(PEER_NAME_PREFIX,
                                                                   '') if '@' in cand_uri else cand_uri[-6:]
                voted_info_list.append(f"E{ep}:P({cand_peer_id_part})")
            except:
                voted_info_list.append(f"E{ep}:{cand_uri[-6:]}")  # Fallback

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
        time.sleep(random.uniform(2, 4))  # Atraso inicial para permitir que a rede se estabilize

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
                    else:
                        tracker_uri_at_cmd = self.current_tracker_uri_str
                        epoch_at_cmd = self.current_tracker_epoch
                        self._clear_current_tracker()
                        self.current_tracker_epoch = epoch_at_cmd
                        self.logger.info(f"Simulando falha do tracker {tracker_uri_at_cmd} (Época {epoch_at_cmd}).")
                        self.initiate_election()
                elif cmd == "quit":
                    self.logger.info("Saindo...")
                    break
                else:
                    if cmd: print("Comando desconhecido.")
            except EOFError:  # Trata Ctrl+D
                self.logger.warning("EOF recebido, encerrando CLI e peer.")
                break  # Sai do loop da CLI, o que levará ao shutdown do peer
            except KeyboardInterrupt:  # Trata Ctrl+C na CLI
                self.logger.info("Interrupção de teclado na CLI. Use 'quit' para sair do peer ou Ctrl+D.")
            except Exception as e:
                self.logger.error(f"Erro no CLI: {e}", exc_info=True)

        # Quando o loop da CLI termina (por 'quit' ou EOF), o peer deve ser encerrado.
        # A chamada para self.shutdown() está no finally do método start().
        # Se o daemon ainda estiver rodando, precisamos pará-lo para que o start() termine.
        if self.pyro_daemon and hasattr(self.pyro_daemon, 'transportServer') and self.pyro_daemon.transportServer:
            self.logger.info("CLI encerrada, solicitando desligamento do daemon Pyro.")
            self.pyro_daemon.shutdown()

    def start(self):
        """Inicia o peer: configura PyRO, descobre tracker e inicia loop do daemon."""
        self._setup_pyro()

        initial_delay = random.uniform(0.5, 2.0)
        if self.peer_id == "Peer1":
            initial_delay = random.uniform(0.1, 0.3)

        self.logger.info(f"Aguardando {initial_delay:.2f}s antes de descobrir o tracker...")
        time.sleep(initial_delay)

        self._discover_tracker()

        cli_thread = threading.Thread(target=self.run_cli, name=f"CLIThread-{self.peer_id}",
                                      daemon=False)  # daemon=False para que o programa espere a CLI
        cli_thread.start()

        self.logger.info(f"Peer {self.peer_id} pronto e aguardando requisições/comandos.")
        try:
            self.pyro_daemon.requestLoop()
        except KeyboardInterrupt:  # Captura Ctrl+C no daemon principal
            self.logger.info(f"Interrupção de teclado recebida no daemon do Peer {self.peer_id}. Encerrando...")
        finally:
            self.logger.info(
                f"Loop do daemon Pyro encerrado para Peer {self.peer_id}. Aguardando CLI thread finalizar...")
            if cli_thread.is_alive():
                self.logger.info(
                    "CLI thread ainda ativa. O programa pode não fechar até que a CLI seja encerrada (com 'quit' ou EOF).")
            self.shutdown()

    def shutdown(self):
        """Encerra o peer de forma limpa."""
        self.logger.info(f"Encerrando Peer {self.peer_id}...")

        self._stop_tracker_timeout_detection()
        self._stop_sending_heartbeats()
        if self.election_vote_collection_timer and self.election_vote_collection_timer.is_alive():
            self.election_vote_collection_timer.cancel()
            self.logger.debug("Timer de coleta de votos da eleição cancelado.")

        # O daemon Pyro já deve ter sido desligado pela CLI ou pelo finally do start()
        if self.pyro_daemon and hasattr(self.pyro_daemon, 'transportServer') and self.pyro_daemon.transportServer:
            self.logger.info("Shutdown: Daemon Pyro ainda parece ativo, desligando agora.")
            self.pyro_daemon.shutdown()  # Garante que está desligado

        # Desregistro do NameServer
        ns_proxy_shutdown = None
        try:
            ns_proxy_shutdown = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
            ns_proxy_shutdown.remove(f"{PEER_NAME_PREFIX}{self.peer_id}")
            self.logger.info(f"Removido {PEER_NAME_PREFIX}{self.peer_id} do servidor de nomes.")
            if self.is_tracker:  # Se era tracker, remove seu registro de tracker
                tracker_name_to_remove = f"{TRACKER_BASE_NAME}{self.current_tracker_epoch}"
                try:
                    registered_uri = ns_proxy_shutdown.lookup(tracker_name_to_remove)
                    if str(registered_uri) == str(self.uri):  # Só remove se o URI for o meu
                        ns_proxy_shutdown.remove(tracker_name_to_remove)
                        self.logger.info(f"Removido {tracker_name_to_remove} (meu registro de tracker) do NS.")
                    else:
                        self.logger.info(
                            f"Não removi {tracker_name_to_remove} do NS, URI ({registered_uri}) não é meu.")
                except Pyro5.errors.NamingError:
                    self.logger.debug(f"{tracker_name_to_remove} não encontrado no NS para remoção (tracker).")
        except Pyro5.errors.NamingError:
            self.logger.debug("Nomes não encontrados no servidor de nomes para remoção (podem já ter sido removidos).")
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
    Pyro5.config.DETAILED_TRACEBACK = True  # Útil para debugging

    peer_instance = Peer(peer_id_arg, shared_folder_arg)
    try:
        peer_instance.start()
    except Exception as main_exc:
        peer_instance.logger.critical(f"Erro crítico no Peer {peer_id_arg} que causou a sua paragem: {main_exc}",
                                      exc_info=True)
    finally:

        peer_instance.logger.info(f"Bloco finally principal alcançado para Peer {peer_id_arg}.")


