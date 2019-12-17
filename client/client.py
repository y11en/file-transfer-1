import socket
import os
import sys
import getopt
import time
import threading

success = []
chunks_list = []
total_downloaded = 0


def client_program(host, port, index, filename):
    global total_downloaded
    global chunks_list
    client_socket = socket.socket()

    try:
        client_socket.connect((host, int(port)))
    except ConnectionRefusedError:
        print(f'Server {port} is not active.')
        return

    start_location = chunks_list[index]["start"]
    start_location += chunks_list[index]["downloaded"]
    end_location = chunks_list[index]["end"]
    client_socket.send(str(start_location).encode())
    client_socket.recv(1024).decode()
    client_socket.send(str(end_location).encode())

    if start_location >= end_location:
        success[index] = True
        return

    try:
        with open(filename, 'r+b') as f:
            f.seek(start_location)
            while True:
                data = client_socket.recv(1024)

                if not data:
                    break

                chunks_list[index]["downloaded"] += len(data)
                total_downloaded += len(data)

                f.write(data)
    except ConnectionResetError:
        print(f'Server {port} has crashed.')
        return

    if chunks_list[index]["downloaded"] + chunks_list[index]["start"] >= end_location:
        success[index] = True

    client_socket.close()


def metric_reporting(filesize, interval):
    global total_downloaded
    global chunks_list

    chunks_count = len(chunks_list)
    servers_last_downloaded = [None] * chunks_count

    while total_downloaded != filesize:
        last_downloaded = total_downloaded
        for i in range(chunks_count):
            servers_last_downloaded[i] = chunks_list[i]["downloaded"]

        time.sleep(interval)

        for i in range(chunks_count):
            speed = (chunks_list[i]["downloaded"] -
                     servers_last_downloaded[i]) / (1024 * 1024 * interval)
            print(
                f'Chunk {1 + i}: {chunks_list[i]["downloaded"]}/{chunks_list[i]["end"] - chunks_list[i]["start"]}. Download speed: {speed} MB/s')

        speed = (total_downloaded - last_downloaded) / (1024 * 1024 * interval)
        print(
            f'Total: {total_downloaded}/{filesize}. Download speed: {speed} MB/s')
        print()


def main(argv):
    # Default values for host, ports, filename, and metric_interval
    host = 'localhost'
    ports = ['8080', '8081', '8082', '8083']
    filename = 'downloaded-vid.mp4'
    metric_interval = 1

    try:
        opts, args = getopt.getopt(argv, "i:o:a:p")
    except getopt.GetoptError:
        print('server.py -i <metric_interval> -o <output_filename> -a <server_ip> -p <list_of_ports>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-o':
            filename = arg
        elif opt == '-a':
            host = arg
        elif opt == '-i':
            metric_interval = int(arg)

    if args:
        ports = args

    # Creating file and getting filesize from server
    open(filename, 'a').close()
    initial_socket = socket.socket()
    initial_socket.connect((host, 1404))
    filesize = int(initial_socket.recv(1024).decode())
    initial_socket.close()
    print(f'Filesize is {filesize}')

    # Once filesize is available, segmentation into chunks
    servers = len(ports)
    for i in range(servers):
        success.append(False)
        chunk_size = int(filesize / servers)
        count = int(chunk_size / 1024) + 1
        chunks_list.append({
            "start": i * count * 1024,
            "end": (i + 1) * count * 1024,
            "downloaded": 0
        })
    chunks_list[len(chunks_list) - 1]["end"] = filesize

    # Run multiple connections concurrently to fetch chunks
    for i in range(servers):
        t = threading.Thread(target=client_program, kwargs={
                             'host': host, 'port': ports[i], 'index': i, 'filename': filename})
        t.setDaemon(True)
        t.start()

    # Run metric reporting thread
    metric_thread = threading.Thread(target=metric_reporting,
                                     kwargs={'filesize': filesize, 'interval': metric_interval})
    metric_thread.setDaemon(True)
    metric_thread.start()

    # Wait for all connections to complete
    main_thread = threading.current_thread()
    for t in threading.enumerate():
        if t is main_thread:
            continue
        if t is metric_thread:
            continue
        t.join()

    # Server failure handler: some chunks didn't receive successfully
    while False in success:
        active_servers = []
        failed_chunks = []

        # Server failure check
        for port in ports:
            try:
                temp_socket = socket.socket()
                temp_socket.connect((host, int(port)))
                temp_socket.send(str(-1).encode())
                temp_socket.close()
                active_servers.append(int(port))
            except ConnectionRefusedError:
                print(f'Server {port} is not active.')

        # Continue reconnecting if all servers down
        # This resumes downloads when connection(s) establishes
        if (len(active_servers)) == 0:
            continue

        # Finding unsuccessful chunks
        for i in range(len(success)):
            if success[i] == False:
                failed_chunks.append(i)

        # Load balancing
        if len(active_servers) > len(failed_chunks):
            for i in range(len(failed_chunks)):
                t = threading.Thread(target=client_program, kwargs={
                                     'host': host, 'port': active_servers[i], 'index': failed_chunks[i], 'filename': filename})
                t.setDaemon(True)
                t.start()
        else:
            for i in range(len(active_servers)):
                t = threading.Thread(target=client_program, kwargs={
                                     'host': host, 'port': active_servers[i], 'index': failed_chunks[i], 'filename': filename})
                t.setDaemon(True)
                t.start()

        # Wait for all threads to complete
        for t in threading.enumerate():
            if t is main_thread:
                continue
            t.join()

    print(f'File downloaded successfully. Size: {total_downloaded} bytes')


if __name__ == '__main__':
    main(sys.argv[1:])
