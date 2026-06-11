import socket
import threading
import json
import time

HOST = '127.0.0.1'
PORT = 65432

# Cấu trúc dữ liệu dùng chung (Shared Structures)
worker_table = {}  # {id: {'status': 'Alive', 'load': 0, 'last_heartbeat': time, 'conn': conn}}
task_queue = []    # Hàng đợi chứa các tác vụ READY
lock = threading.Lock() # Mutex để đồng bộ giữa các luồng xử lý song song

# Luồng 1: Tiếp nhận kết nối mạng và xử lý các bản tin JSON từ Worker
def handle_worker(conn, addr):
    global worker_table, task_queue
    while True:
        try:
            data = conn.recv(1024).decode('utf-8')
            if not data: break
            
            message = json.loads(data)
            msg_type = message.get("type")
            
            with lock: # Bắt đầu Critical Section (Vùng thiết yếu)
                if msg_type == "REGISTER":
                    w_id = message["worker_id"]
                    worker_table[w_id] = {
                        'status': 'Alive', 
                        'load': 0, 
                        'last_heartbeat': time.time(), 
                        'conn': conn
                    }
                    print(f"[MASTER] Worker {w_id} registered.")
                    
                elif msg_type == "HEARTBEAT":
                    w_id = message["worker_id"]
                    if w_id in worker_table and worker_table[w_id]['status'] == 'Alive':
                        worker_table[w_id]['last_heartbeat'] = time.time()
                        
                elif msg_type == "RESULT":
                    t_id = message["task_id"]
                    print(f"[MASTER] Task {t_id} COMPLETED. Result: {message['output']}")
                    # Giảm tải cho Worker sau khi hoàn thành tác vụ
                    for w_id, info in worker_table.items():
                        if info['conn'] == conn:
                            info['load'] = max(0, info['load'] - 1)
                            
        except:
            break
    conn.close()

# Luồng 2: Bộ điều phối (Scheduler) - Chiến lược Least Loaded
def scheduler_thread():
    while True:
        time.sleep(1) # Định kỳ quét hàng đợi mỗi giây
        with lock:
            if not task_queue: continue
            
            # Tìm các Worker đang hoạt động ổn định
            alive_workers = {k: v for k, v in worker_table.items() if v['status'] == 'Alive'}
            if not alive_workers: continue
            
            # Thuật toán Least Loaded: Chọn Worker có Load (số việc đang chạy) nhỏ nhất
            best_worker_id = min(alive_workers, key=lambda k: alive_workers[k]['load'])
            
            task = task_queue.pop(0) # FIFO lấy tác vụ đầu hàng đợi
            worker_conn = alive_workers[best_worker_id]['conn']
            
            try:
                worker_conn.sendall(json.dumps(task).encode('utf-8'))
                alive_workers[best_worker_id]['load'] += 1 # Tăng tải quản lý
                print(f"[SCHEDULER] Assigned Task {task['task_id']} to Worker {best_worker_id}")
            except:
                task_queue.insert(0, task) # Đẩy lại vào đầu hàng đợi nếu gặp lỗi mạng

# Luồng 3: Giám sát Heartbeat và Phát hiện lỗi (Failure Detection)
def heartbeat_monitor():
    while True:
        time.sleep(2)
        current = time.time()
        with lock:
            for w_id, info in list(worker_table.items()):
                # Nếu Quá 6 giây không nhận được Heartbeat từ Worker -> Đánh dấu FAILED
                if info['status'] == 'Alive' and (current - info['last_heartbeat'] > 6):
                    print(f"[⚠️ DETECTED FAILURE] Worker {w_id} is down!")
                    info['status'] = 'FAILED'

def start_master():
    global task_queue
    # Nạp sẵn danh sách tác vụ với Input vừa phải để tính toán nhanh
    task_queue = [
        {"type": "TASK", "task_id": 101, "operation": "prime_count", "input": 5000},
        {"type": "TASK", "task_id": 102, "operation": "prime_count", "input": 7000},
        {"type": "TASK", "task_id": 103, "operation": "prime_count", "input": 9000}
    ]
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()
    print(f"[MASTER] Node started on {HOST}:{PORT}. Waiting for workers...")
    
    # Kích hoạt các luồng chức năng chạy ngầm (Daemon Threads)
    threading.Thread(target=scheduler_thread, daemon=True).start()
    threading.Thread(target=heartbeat_monitor, daemon=True).start()
    
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_worker, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    start_master()
