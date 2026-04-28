import grpc
import threading
import chat_pb2
import chat_pb2_grpc

def generate_messages(username):
    print("Digite sua mensagem. Pressione Ctrl+C para sair.")
    while True:
        msg = input()
        if msg.strip() == "":
            continue
        yield chat_pb2.ChatMessage(user=username, message=msg)

def receive_messages(responses, stop_event):
    try:
        for response in responses:
            print(f"\n[{response.user}]: {response.message}")
    except grpc.RpcError as error:
        if not stop_event.is_set():
            print(f"Erro na conexão: {error}")
    finally:
        stop_event.set()

def run():
    channel = grpc.insecure_channel("localhost:50051")
    stub = chat_pb2_grpc.ChatServiceStub(channel)

    username = input("Digite seu nome: ")
    stop_event = threading.Event()

    responses = stub.Chat(generate_messages(username))
    receiver_thread = threading.Thread(target=receive_messages, args=(responses, stop_event), daemon=True)
    receiver_thread.start()

    try:
        while not stop_event.is_set():
            stop_event.wait(0.1)
    except KeyboardInterrupt:
        print("\nSaindo...")
        stop_event.set()

    receiver_thread.join()

if __name__ == "__main__":
    run()