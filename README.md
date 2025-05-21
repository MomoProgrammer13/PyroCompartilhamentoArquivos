# Sistema de Compartilhamento de Arquivos P2P com Pyro5

Este projeto implementa um sistema de compartilhamento de arquivos peer-to-peer (P2P) utilizando a biblioteca Pyro5 (Python Remote Objects). Ele apresenta funcionalidades como descoberta de arquivos, eleição dinâmica de um peer "tracker" e download direto de arquivos entre peers.

## Funcionalidades Principais

*   **Compartilhamento de Arquivos**: Peers podem compartilhar arquivos localizados em suas pastas designadas.
*   **Descoberta de Tracker Dinâmica**: Um peer é eleito como "tracker" para manter um índice dos arquivos disponíveis na rede e quais peers os possuem.
*   **Eleição de Tracker**: Se o tracker atual falhar, os peers iniciam um processo de eleição para escolher um novo tracker. Este processo utiliza um sistema de épocas e requer um quórum de votos.
*   **Heartbeats e Detecção de Falhas**: O tracker envia heartbeats periódicos. Os peers monitoram esses heartbeats e, na ausência deles, podem iniciar uma nova eleição.
*   **Download P2P**: Após descobrir quem possui um arquivo através do tracker, o download é realizado diretamente do peer detentor.
*   **Interface de Linha de Comando (CLI)**: Cada peer possui uma CLI para interagir com a rede (buscar arquivos, listar arquivos, verificar status, etc.).
*   **Logging**: Cada peer gera um arquivo de log individual para facilitar o debugging e acompanhamento.

## Tecnologias Utilizadas

*   Python 3.x
*   Pyro5: Para comunicação via objetos remotos.

## Pré-requisitos

*   Python 3.6 ou superior.
*   Pyro5: Instale com `pip install Pyro5`.



## Como Executar

**1. Instale as dependências:**

```bash
pip install Pyro5
```

**2. Execute o script `run_peers.py`:**

Este script irá:
*   Limpar e criar as pastas `p2p_shared_folders`, `p2p_download_folders` e `logs`.
*   Popular as pastas compartilhadas dos peers com arquivos de exemplo.
*   Iniciar o servidor de nomes Pyro5.
*   Iniciar o número de peers especificado em `TOTAL_PEERS_EXPECTED` (em `constants.py`).

```bash
python run_peers.py
```

No Windows, se o Windows Terminal (`wt.exe`) e PowerShell estiverem disponíveis, cada peer será aberto em uma nova aba do Windows Terminal com dois painéis: um para a CLI do peer e outro para visualização ao vivo dos logs. Caso contrário, cada peer será iniciado em uma nova janela de console (CMD), e os logs estarão nos arquivos dentro da pasta `logs/`.

**3. Interaja com os Peers:**

Cada peer terá sua própria janela de console (ou painel no Windows Terminal) com uma interface de linha de comando (CLI). Você pode usar os seguintes comandos:

*   `search`: Busca um arquivo na rede e oferece a opção de download.
*   `list my`: Lista os arquivos compartilhados localmente por aquele peer.
*   `list net`: Lista todos os arquivos disponíveis na rede (conforme indexado pelo tracker).
*   `refresh`: Reexamina a pasta compartilhada local e notifica o tracker sobre quaisquer mudanças.
*   `status`: Mostra o status atual do peer, incluindo se é o tracker, qual tracker conhece, e informações de eleição.
*   `election`: Força o início de uma eleição (simula uma falha do tracker). Útil para testar a robustez do sistema.
*   `quit`: Encerra o peer.

**4. Encerrando a Simulação:**

*   Você pode digitar `quit` na CLI de cada peer para encerrá-los individualmente.
*   Pressionar `Ctrl+C` na janela onde `run_peers.py` foi executado tentará encerrar todos os processos de peers e o servidor de nomes.

## Funcionamento Detalhado

### Tracker

*   Um dos peers é eleito dinamicamente como o "tracker".
*   O tracker é responsável por manter um índice de quais arquivos estão disponíveis na rede e quais peers possuem cada arquivo.
*   Peers registram seus arquivos compartilhados com o tracker.
*   Quando um peer deseja encontrar um arquivo, ele consulta o tracker.

### Eleição de Tracker

*   **Início**: Uma eleição é iniciada se um peer não consegue encontrar um tracker, se o tracker atual para de enviar heartbeats, ou se um peer detecta um conflito de tracker (e.g., dois trackers na mesma época com URIs diferentes, ou um tracker com época superior).
*   **Épocas**: Cada tracker (e cada eleição) está associado a um número de "época". Uma nova eleição sempre tentará estabelecer um tracker para uma época superior à última conhecida. Isso ajuda a resolver conflitos e garante que os peers convirjam para o tracker mais recente.
*   **Votação**:
    *   Um peer candidato a tracker envia pedidos de voto para outros peers para uma nova época.
    *   Um peer votará em um candidato se:
        1.  A época da eleição for maior ou igual à época do tracker que ele conhece.
        2.  Ele ainda não votou naquela época, ou se votou, o novo candidato tem um critério de desempate favorável (e.g., URI lexicograficamente menor, caso o peer tenha votado em si mesmo anteriormente para a mesma época).
    *   Um peer só pode votar uma vez por época (com a exceção da regra de desempate mencionada).
*   **Quórum**: O candidato precisa receber um número mínimo de votos (definido por `QUORUM` em `constants.py`) para se tornar o tracker.
*   **Registro no Servidor de Nomes**: Uma vez eleito, o novo tracker registra-se no servidor de nomes Pyro com um nome que inclui sua época (e.g., `p2p.tracker.epoch.5`).

### Heartbeats e Detecção de Falhas

*   **Envio (Tracker)**: O tracker ativo envia mensagens de "heartbeat" periodicamente para todos os outros peers conhecidos.
*   **Recebimento (Peer)**:
    *   Quando um peer recebe um heartbeat, ele sabe que o tracker está ativo e reinicia um timer de timeout.
    *   Se um peer receber um heartbeat de um tracker com uma época superior à do seu tracker conhecido, ele mudará para o novo tracker.
    *   Se um peer receber um heartbeat de um tracker diferente, mas na mesma época do seu tracker conhecido, um desempate (baseado no URI do tracker) é usado.
*   **Timeout (Peer)**: Se um peer não receber um heartbeat do seu tracker atual dentro de um período de tempo (aleatório entre `TRACKER_DETECTION_TIMEOUT_MIN` e `TRACKER_DETECTION_TIMEOUT_MAX`), ele considera o tracker como falho e inicia uma nova eleição.
*   **Conflito (Tracker)**: Se um peer que é tracker recebe um heartbeat de outro tracker:
    *   Se o outro tracker tiver uma época maior, o tracker atual renuncia.
    *   Se o outro tracker tiver a mesma época, mas um URI "menor" (critério de desempate), o tracker atual renuncia.

### Compartilhamento e Download de Arquivos

1.  **Registro**: Quando um peer inicia ou atualiza seus arquivos locais (via comando `refresh`), ele notifica o tracker atual, enviando sua lista de arquivos. O tracker atualiza seu índice.
2.  **Busca**: Um peer usa o comando `search <nome_do_arquivo>` na CLI.
    *   O peer contata o tracker e pergunta quem possui o arquivo.
    *   O tracker responde com uma lista de peers (ID e URI) que possuem o arquivo.
3.  **Download**:
    *   O peer solicitante escolhe um dos peers da lista.
    *   Ele então se conecta diretamente ao peer detentor usando Pyro5.
    *   O arquivo é transferido em chunks (partes) para permitir o download de arquivos grandes e fornecer feedback de progresso.
    *   O arquivo baixado é salvo na pasta `p2p_download_folders/<ID_do_Peer_que_baixou>/`.
    *   Após o download, o peer atualiza sua lista de arquivos locais e notifica o tracker.

Este sistema demonstra conceitos importantes de sistemas distribuídos, como descoberta de serviços, eleição de líder, detecção de falhas e comunicação P2P.
