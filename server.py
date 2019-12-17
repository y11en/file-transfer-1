import socket
import multiprocessing
import os
import sys
import getopt
import time


# A file server that receives start/end location from client
# then delivers that chunk of data only
# Multiple file servers will be run in main function below
def server_program(host, port, filename):
    while True:
        server_socket = socket.socket()
        server_socket.bind((host, int(port)))
        # print(f'File server listening at {host}:{port}')

        server_socket.listen(1)
        conn, addr = server_socket.accept()

        start_location = int(conn.recv(1024).decode())

        if (start_location == -1):
            conn.close()
            continue

        conn.send('Chunk received!'.encode())
        end_location = int(conn.recv(1024).decode())

        with open(filename, 'rb') as f:
            f.seek(start_location)
            while f.tell() < end_location:
                buf = f.read(1024)

                if not buf:
                    break

                conn.send(buf)

        conn.close()


# A initial server that sends the filesize to client
# It runs on a pre-defined port 1404
def initial_server(host, filename):
    filesize = os.stat(filename).st_size
    while True:
        initial_socket = socket.socket()
        initial_socket.bind((host, 1404))
        # print(f'Initial server listening at {host}:1404')
        initial_socket.listen(1)
        conn, addr = initial_socket.accept()
        conn.send(str(filesize).encode())
        conn.close()


def main(argv):
    # Default values for host, ports, and filename
    host = 'localhost'
    ports = ['8080', '8081', '8082', '8083']
    filename = 'stranger.mp4'

    try:
        opts, args = getopt.getopt(argv, "f:n:p")
    except getopt.GetoptError:
        print('server.py -f <filename> -n <num_servers> -p <list_of_ports>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-f':
            filename = arg

    if args:
        ports = args

    host_name = socket.gethostname()
    host = socket.gethostbyname(host_name)
    print(f'Server will run at {host}')
    print(f'Server will host file {filename}')
    print()

    server_processes = {}

    server_processes['1404'] = multiprocessing.Process(
        target=initial_server, args=(host, filename,))
    server_processes['1404'].start()

    for i in range(len(ports)):
        process = multiprocessing.Process(
            target=server_program, args=(host, ports[i], filename))
        process.start()
        server_processes[ports[i]] = process

    time.sleep(1)

    while True:
        i = 1

        for port in server_processes:
            if port == '1404':
                continue
            status = 'alive' if server_processes[port].is_alive() else 'dead'
            print(f'File server {i}, Port: {port}, Status: {status}')
            i += 1

        shutdown_port = input('Enter port number to shutdown: ')

        for port in server_processes:
            if port == shutdown_port:
                server_processes[port].terminate()

        time.sleep(0.1)
        print()


if __name__ == '__main__':
    main(sys.argv[1:])
