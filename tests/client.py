import socket


class EkilibriClient:
    HOST = "localhost"

    def connect(self, port: int = 8080):
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__socket.connect((self.HOST, port))

    def send_and_receive(self, message: str):
        self.__socket.send(message.encode("utf-8"))
        return self.__socket.recv(1024)
