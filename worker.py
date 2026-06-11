import socket
import threading
import json
import time
import sys

HOST = '127.0.0.1'
PORT = 65432

def count_primes(n):
    count = 0
    for num in range(2, n + 1):
        if num % 5000 == 0:
            time.sleep(0.001)
            
        if all(num % i != 0 for i in range(2, int(num**0.5) + 1)):
            count += 1
    return count

def heartbeat_sender(sock, worker_id):
    while True:
        try:
            heartbeat_msg = {"type": "HEARTBEAT", "worker_id": worker_id}
            sock.sendall(json.dumps(heartbeat_msg).encode('utf-8'))
            time.sleep(2)
        except:
            print(f"[WORKER {worker_id}] Lost connection to master. Stopping heartbeat.")
            break

def start_worker(worker_id):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((HOST, PORT))
    except Exception as e:
        print(f"[WORKER] Failed to connect to Master: {e}")
        return

    register_msg = {"type": "REGISTER", "worker_id": worker_id}
    sock.sendall(json.dumps(register_msg).encode('utf-8'))
    print(f"[WORKER {worker_id}] Registered successfully.")
    
    threading.Thread(target=heartbeat_sender, args=(sock, worker_id), daemon=True).start()
    
    while True:
        try:
            data = sock.recv(1024).decode('utf-8')
            if not data: break
            
            task = json.loads(data)
            if task.get("type") == "TASK":
                t_id = task["task_id"]
                n = task["input"]
                print(f"[WORKER {worker_id}] Processing Task {t_id} (N={n})...")
                
                result = count_primes(n)
                
                result_msg = {"type": "RESULT", "task_id": t_id, "output": result}
                sock.sendall(json.dumps(result_msg).encode('utf-8'))
                print(f"[WORKER {worker_id}] Sent result for Task {t_id}.")
        except:
            break
    sock.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Vui lòng nhập ID Worker! Ví dụ: python3 worker.py 1")
        sys.exit(1)
    start_worker(int(sys.argv[1]))
