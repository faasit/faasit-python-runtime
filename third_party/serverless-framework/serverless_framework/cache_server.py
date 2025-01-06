from .kv_cache import KVCache

import socket
import threading
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
import logging

class Reply:
    def __init__(self, msg: Optional[str], obj: Optional[bytes]):
        assert(msg is not None or obj is not None)
        assert(msg is None or obj is None)
        self.msg = msg
        self.obj = obj

    def is_msg(self) -> bool:
        return self.msg is not None
    
    def is_obj(self) -> bool:
        return self.obj is not None

    def to_bytes(self) -> bytes:
        if self.msg is not None:
            return b"===msg: " + self.msg.encode(encoding='utf-8')
        else:
            assert(self.obj)
            return b"===obj: " + self.obj

    @staticmethod
    def from_bytes(data: bytes) -> 'Reply':
        if data.startswith(b"===msg: "):
            return Reply(data[8:].decode(encoding='utf-8'), None)
        elif data.startswith(b"===obj: "):
            return Reply(None, data[8:])
        else:
            raise ValueError("Invalid reply format")
    
class CacheServer:
    def __init__(self, cache: KVCache, port: int, timeout = 5):
        self.port = port
        self.cache = cache
        self.timeout = timeout

        self._shutdown_lock = threading.Lock()
        self._shutdown = False

        self._listen_thr: Optional[threading.Thread] = None
        self.pool = ThreadPoolExecutor(max_workers=10)

    def _handle_conn(self, conn: socket.socket, addr: str):
        try:
            with conn:
                data = conn.recv(1024)
                if not data:
                    return
                if len(data) > 512:
                    conn.sendall(Reply("Request too long.", None).to_bytes())
                    return
                
                key = data.decode()
                logging.debug(f"Cache server handling request from addr={addr}, key={key}")
                bytes = self.cache.get_bytes(key)

                if bytes is None:
                    logging.warning(f"Key not found: {key}")
                    conn.sendall(Reply("Key not found.", None).to_bytes())
                    return
                conn.sendall(Reply(None, bytes).to_bytes())

        except Exception as e:
            logging.warning(f"Error occurred while cache server handling connection from {addr}: {str(e)}")


    def _listener(self):
        sket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Add SO_REUSEADDR option
        sket.bind(('0.0.0.0', self.port))
        sket.listen(10)
        sket.settimeout(self.timeout)
        
        while True:
            with self._shutdown_lock:
                if self._shutdown:
                    break
            try:
                conn, addr = sket.accept()
            except socket.timeout:
                continue

            conn.settimeout(self.timeout)
            self.pool.submit(self._handle_conn, conn, addr)
            logging.debug(f"Cache server accepted connection from {addr}. queued task: {self.pool._work_queue.qsize()}")
        sket.close()

    def start(self):
        logging.info(f"Cache server starts listening on port {self.port}")
        assert(self._listen_thr is None)
        self.shutdown = False
        self._listen_thr = threading.Thread(target=self._listener)
        self._listen_thr.start()

    def stop(self):
        assert(self._listen_thr is not None)
        with self._shutdown_lock:
            self._shutdown = True
        self._listen_thr.join()
        self.pool.shutdown()