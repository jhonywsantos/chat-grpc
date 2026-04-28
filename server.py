import grpc
from concurrent import futures
import queue
import threading
import chat_pb2
import chat_pb2_grpc

clients = []
clients_lock = threading.Lock()

class ChatService(chat_pb2_grpc.ChatServiceServicer):
    def Chat(self, request_iterator, context):
        client_queue = queue.Queue()

        with clients_lock:
            clients.append(client_queue)

        def broadcast(message, exclude_queue=None):
            with clients_lock:
                for q in clients:
                    if q is exclude_queue:
                        continue
                    q.put(message)

        def read_requests():
            try:
                for message in request_iterator:
                    print(f"[{message.user}]: {message.message}")
                    broadcast(message, exclude_queue=client_queue)
            finally:
                client_queue.put(None)

        threading.Thread(target=read_requests, daemon=True).start()

        try:
            while True:
                next_message = client_queue.get()
                if next_message is None:
                    break
                yield next_message
        finally:
            with clients_lock:
                if client_queue in clients:
                    clients.remove(client_queue)


def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chat_pb2_grpc.add_ChatServiceServicer_to_server(ChatService(), server)

    server.add_insecure_port(f"[::]:{port}")
    server.start()

    print(f"Servidor rodando na porta {port}")
    server.wait_for_termination()

if __name__ == "__main__":
    serve(50051)