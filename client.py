import grpc
import threading
import chat_pb2
import chat_pb2_grpc

def generate_messages(username):
    while True:
        msg = input()
        yield chat_pb2.ChatMessage(user=username, message=msg)

def receive_messages(responses):
    for response in responses:
        print(f"\n[{response.user}]: {response.message}")

def run():
    channel = grpc.insecure_channel("localhost:50051")
    stub = chat_pb2_grpc.ChatServiceStub(channel)

    username = input("Digite seu nome: ")

    responses = stub.Chat(generate_messages(username))

    # Thread para receber mensagens
    threading.Thread(target=receive_messages, args=(responses,), daemon=True).start()

    # Mantém envio ativo
    while True:
        pass

if __name__ == "__main__":
    run()