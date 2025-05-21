# constants.py

# Configurações do Servidor de Nomes PyRO
NAMESERVER_HOST = "localhost"
NAMESERVER_PORT = 9090

# Nomes base para registro no serviço de nomes
PEER_NAME_PREFIX = "P2P_Peer_"
TRACKER_BASE_NAME = "P2P_Tracker_Epoca_"

# Configurações de tempo (em segundos)
HEARTBEAT_INTERVAL = 0.1  # Intervalo para o tracker enviar heartbeats (100 ms)
# Timeout aleatório para um peer detectar falha no tracker (entre 150–300 ms)
TRACKER_DETECTION_TIMEOUT_MIN = 0.15
TRACKER_DETECTION_TIMEOUT_MAX = 0.3
ELECTION_REQUEST_TIMEOUT = 3.0 # Timeout para esperar por votos

# Configurações da rede P2P
TOTAL_PEERS_EXPECTED = 5 # Número total de peers na rede (para cálculo do quórum)
# Quórum necessário para eleger um novo tracker. (N/2) + 1
QUORUM = TOTAL_PEERS_EXPECTED // 2 + 1

# Outras constantes
MAX_EPOCH_SEARCH = 100 # Ao buscar um tracker, até qual época procurar
DOWNLOAD_CHUNK_SIZE = 1024 * 1024 # 1MB por chunk para download
