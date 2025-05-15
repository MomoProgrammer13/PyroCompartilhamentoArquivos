# run_peers.py
import subprocess
import time
import os
import sys
import shutil
from constants import NAMESERVER_HOST, NAMESERVER_PORT, TOTAL_PEERS_EXPECTED

# --- Configurações ---
PYTHON_EXECUTABLE = sys.executable  # Usa o mesmo executável Python que está rodando este script
PEER_SCRIPT_PATH = "peer.py"  # Caminho para o script peer.py
BASE_SHARED_DIR = "p2p_shared_folders"  # Pasta base para os diretórios compartilhados dos peers
BASE_DOWNLOAD_DIR = "p2p_download_folders"  # Pasta base para os downloads dos peers

# Arquivos de exemplo para popular as pastas dos peers
EXAMPLE_FILES_CONTENT = {
    "fileA.txt": "Conteúdo do arquivo A.",
    "fileB.txt": "Conteúdo do arquivo B.",
    "fileC.txt": "Conteúdo do arquivo C.",
    "common.txt": "Este é um arquivo comum a vários peers."
}


def start_nameserver():
    """Inicia o servidor de nomes PyRO em um novo processo."""
    print(f"Iniciando servidor de nomes PyRO em {NAMESERVER_HOST}:{NAMESERVER_PORT}...")
    try:
        # Tenta verificar se já está rodando para evitar erro, mas é uma verificação simples.
        # Idealmente, o próprio nameserver lida com múltiplas instâncias ou falha graciosamente.
        ns_process = subprocess.Popen(
            [PYTHON_EXECUTABLE, "-m", "Pyro5.nameserver", "-n", NAMESERVER_HOST, "-p", str(NAMESERVER_PORT)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL  # Suprime a saída do nameserver
        )
        print(f"Servidor de nomes iniciado (PID: {ns_process.pid}). Aguardando inicialização...")
        time.sleep(3)  # Dá um tempo para o servidor de nomes iniciar
        return ns_process
    except Exception as e:
        print(f"Erro ao iniciar o servidor de nomes: {e}")
        print("Certifique-se que Pyro5 está instalado e o comando 'python -m Pyro5.nameserver' funciona.")
        return None


def create_shared_folders_and_files(num_peers):
    """Cria pastas compartilhadas e arquivos de exemplo para cada peer."""
    if os.path.exists(BASE_SHARED_DIR):
        print(f"Limpando diretório base de compartilhamento antigo: {BASE_SHARED_DIR}")
        shutil.rmtree(BASE_SHARED_DIR)
    os.makedirs(BASE_SHARED_DIR, exist_ok=True)

    if os.path.exists(BASE_DOWNLOAD_DIR):
        print(f"Limpando diretório base de downloads antigo: {BASE_DOWNLOAD_DIR}")
        shutil.rmtree(BASE_DOWNLOAD_DIR)
    os.makedirs(BASE_DOWNLOAD_DIR, exist_ok=True)

    for i in range(1, num_peers + 1):
        peer_folder_name = f"peer{i}_files"
        peer_shared_path = os.path.join(BASE_SHARED_DIR, peer_folder_name)
        os.makedirs(peer_shared_path, exist_ok=True)

        # Cria alguns arquivos de exemplo
        with open(os.path.join(peer_shared_path, f"unique_to_peer{i}.txt"), 'w') as f:
            f.write(f"Este é um arquivo único do Peer{i}.")

        if i % 2 == 0:  # Peers pares têm fileA
            with open(os.path.join(peer_shared_path, "fileA.txt"), 'w') as f:
                f.write(EXAMPLE_FILES_CONTENT["fileA.txt"] + f" (do Peer{i})")
        if i % 2 != 0:  # Peers ímpares têm fileB
            with open(os.path.join(peer_shared_path, "fileB.txt"), 'w') as f:
                f.write(EXAMPLE_FILES_CONTENT["fileB.txt"] + f" (do Peer{i})")
        if i == 1 or i == num_peers:  # Peer 1 e o último têm fileC
            with open(os.path.join(peer_shared_path, "fileC.txt"), 'w') as f:
                f.write(EXAMPLE_FILES_CONTENT["fileC.txt"] + f" (do Peer{i})")

        with open(os.path.join(peer_shared_path, "common.txt"), 'w') as f:
            f.write(EXAMPLE_FILES_CONTENT["common.txt"] + f" (do Peer{i})")

        print(f"Pasta compartilhada criada para Peer{i}: {peer_shared_path}")


def start_peers(num_peers):
    """Inicia múltiplos processos de peers."""
    peer_processes = []
    print(f"\nIniciando {num_peers} peers...")
    for i in range(1, num_peers + 1):
        peer_id = f"Peer{i}"
        shared_folder_path = os.path.join(BASE_SHARED_DIR, f"peer{i}_files")

        # Para Windows, pode ser necessário ajustar como o subprocess é chamado
        # ou garantir que o ambiente Python esteja configurado corretamente.
        # O uso de `creationflags=subprocess.CREATE_NEW_CONSOLE` abre cada peer em um novo console no Windows.
        # No Linux/macOS, eles rodarão em background ou compartilharão o terminal, dependendo da config.
        flags = 0
        if os.name == 'nt':  # Windows
            flags = subprocess.CREATE_NEW_CONSOLE

        try:
            cmd = [PYTHON_EXECUTABLE, PEER_SCRIPT_PATH, peer_id, shared_folder_path]
            print(f"Executando comando: {' '.join(cmd)}")
            peer_process = subprocess.Popen(cmd, creationflags=flags)
            peer_processes.append(peer_process)
            print(f"{peer_id} iniciado (PID: {peer_process.pid}).")
            time.sleep(0.5)  # Pequena pausa entre o início dos peers
        except Exception as e:
            print(f"Erro ao iniciar {peer_id}: {e}")

    return peer_processes


if __name__ == "__main__":
    # Verifica se o script peer.py existe
    if not os.path.exists(PEER_SCRIPT_PATH):
        print(f"Erro: O script '{PEER_SCRIPT_PATH}' não foi encontrado no diretório atual.")
        print("Certifique-se de que 'peer.py' está na mesma pasta que 'run_peers.py'.")
        sys.exit(1)

    num_peers_to_start = TOTAL_PEERS_EXPECTED

    # 1. (Opcional) Limpa e cria pastas compartilhadas e de download
    print("Configurando pastas compartilhadas e de download...")
    create_shared_folders_and_files(num_peers_to_start)

    # 2. Inicia o servidor de nomes
    ns_proc = start_nameserver()
    if not ns_proc:
        print("Não foi possível iniciar o servidor de nomes. Encerrando.")
        sys.exit(1)

    # 3. Inicia os peers
    peer_procs = start_peers(num_peers_to_start)
    if not peer_procs:
        print("Nenhum peer foi iniciado. Encerrando.")
        if ns_proc: ns_proc.terminate()  # Tenta fechar o nameserver
        sys.exit(1)

    print(f"\n{len(peer_procs)} peers e o servidor de nomes estão rodando.")
    print("Cada peer possui sua própria interface de linha de comando (CLI).")
    print("Se os peers foram abertos em novos consoles (Windows), interaja com eles lá.")
    print("Use 'quit' na CLI de cada peer para encerrá-los individualmente.")
    print("Pressione Ctrl+C nesta janela para tentar encerrar todos os peers e o servidor de nomes.")

    try:
        # Mantém o script principal rodando para que Ctrl+C possa pegar
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nSinal de interrupção recebido. Encerrando todos os processos...")
    finally:
        for p in peer_procs:
            try:
                p.terminate()  # Tenta encerrar graciosamente
                p.wait(timeout=5)  # Espera um pouco
            except subprocess.TimeoutExpired:
                p.kill()  # Força o encerramento se necessário
            except Exception:
                pass  # Ignora outros erros no encerramento

        if ns_proc:
            try:
                ns_proc.terminate()
                ns_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                ns_proc.kill()
            except Exception:
                pass

        print("Todos os processos foram sinalizados para encerrar.")
        # Limpeza final das pastas (opcional)
        # print(f"Limpando diretório base de compartilhamento: {BASE_SHARED_DIR}")
        # shutil.rmtree(BASE_SHARED_DIR, ignore_errors=True)
        # print(f"Limpando diretório base de downloads: {BASE_DOWNLOAD_DIR}")
        # shutil.rmtree(BASE_DOWNLOAD_DIR, ignore_errors=True)

