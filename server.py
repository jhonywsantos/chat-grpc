import grpc
from concurrent import futures
import chat_pb2
import chat_pb2_grpc

class ChatService(chat_pb2_grpc.ChatServiceServicer):
    def Chat(self, request_iterator, context):
        for message in request_iterator:
            print(f"[{message.user}]: {message.message}")
            
            # envia de volta (eco simples)
            yield chat_pb2.ChatMessage(
                user="Servidor",
                message=f"Recebido: {message.message}"
            )

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chat_pb2_grpc.add_ChatServiceServicer_to_server(ChatService(), server)
    
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    
    print(f"Servidor rodando na porta {port}")
    server.wait_for_termination()

if __name__ == "__main__":
    serve(50051)