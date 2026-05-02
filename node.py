import grpc
from concurrent import futures
import queue
import threading
import chat_pb2
import chat_pb2_grpc
import sys

# Fila global para mensagens que EU digito
my_messages_to_send = queue.Queue()
# Lista de filas para enviar para quem se conectar a mim (meus clientes)
connected_clients_queues = []
clients_lock = threading.Lock()

class ChatService(chat_pb2_grpc.ChatServiceServicer):
    def Chat(self, request_iterator, context):
        # Cada pessoa que se conecta a mim ganha uma fila própria
        q = queue.Queue()
        with clients_lock:
            connected_clients_queues.append(q)

        # Thread para RECEBER o que o outro nó está me enviando
        def receive_from_peer():
            try:
                for msg in request_iterator:
                    print(f"\n[{msg.user}]: {msg.message}")
                    # Opcional: Re-transmitir para outros (mesh)
            except Exception: pass
            finally:
                q.put(None)

        threading.Thread(target=receive_from_peer, daemon=True).start()

        # O gRPC vai "yieldar" (enviar) o que cair na fila 'q'
        while True:
            msg = q.get()
            if msg is None: break
            yield msg

        with clients_lock:
            connected_clients_queues.remove(q)

# Thread para ler o teclado SEMPRE, independente de ser cliente ou servidor
def keyboard_listener(username):
    while True:
        text = input() # O input fica aqui, sem travar o gRPC
        if text.strip():
            new_msg = chat_pb2.ChatMessage(user=username, message=text)
            
            # 1. Manda para a fila do meu "lado cliente" (se eu estiver conectado a alguém)
            my_messages_to_send.put(new_msg)
            
            # 2. Manda para todos que se conectaram no meu "lado servidor"
            with clients_lock:
                for q in connected_clients_queues:
                    q.put(new_msg)

def start_server(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chat_pb2_grpc.add_ChatServiceServicer_to_server(ChatService(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()

def message_generator():
    while True:
        msg = my_messages_to_send.get()
        if msg is None: break
        yield msg

def run_node():
    my_port = input("Sua porta (ex: 50051): ")
    target_port = input("Porta do colega (vazio se for o primeiro): ")
    username = input("Seu nome: ")

    # 1. Inicia o Servidor (Background)
    threading.Thread(target=start_server, args=(my_port,), daemon=True).start()

    # 2. Inicia o Teclado (Background) - Isso permite que você digite SEMPRE
    threading.Thread(target=keyboard_listener, args=(username,), daemon=True).start()

    # 3. Lógica de Conexão (Lado Cliente)
    if target_port:
        channel = grpc.insecure_channel(f"localhost:{target_port}")
        stub = chat_pb2_grpc.ChatServiceStub(channel)
        
        # O gerador agora pega da FILA, não do input() direto
        responses = stub.Chat(message_generator())
        
        try:
            for response in responses:
                print(f"\n[{response.user}]: {response.message}")
        except grpc.RpcError:
            print("\nConexão perdida com o colega.")
    else:
        print(f"Aguardando conexões em {my_port}...")
        # Se for o primeiro nó, fica apenas esperando e ouvindo o teclado
        while True:
            import time
            time.sleep(1)

if __name__ == "__main__":
    run_node()