import urllib.request
import pickle
import time
import logging
import socket
from typing import Dict, List, Any, Optional
from .cache_server import Reply


def PostUntil200(url: str, data: Any, *, timeout: float = 10, post_ratio: float = 0.0) -> Any:
    """
    POST data to the url until 200 or timeout.
    raise exception if timeout.
    """

    if url.endswith(':'):
        # specially handle the case that port = ''
        url=url[:-1]

    req = urllib.request.Request(url, method='POST')
    req.add_header('Content-Type', 'application/octet-stream')
    req.data = pickle.dumps(data)

    logging.debug(f"POST to {url}, data size: {len(req.data)}, data: {data}")

    start_time = time.time()

    sleep_gap = 0.1
    timeout_in_use = 1.0 if post_ratio == 0.0 else timeout * post_ratio
    while time.time() - start_time < timeout:
        try:
            with urllib.request.urlopen(req, timeout = timeout_in_use) as response:
                
                if response.status != 200:
                    logging.warning(f"POST reponse status is not OK: {response.status}")
                    continue

                data = response.read()
                logging.debug(f"POST to {url} succeeds! Reply data size: {len(data)}")
                return None if len(data) == 0 else pickle.loads(data)

        except Exception as e:

            # if connection refuse
            if str(e) == '<urlopen error [Errno 111] Connection refused>':
                # output connection refuse error only debug-level
                logging.debug(f"Error occurred while post to {url}: {str(e)}. Retrying...")
            else:
                logging.warning(f"Error occurred while post to {url}: {str(e)}. Retrying...")
            
            sleep_gap = min(sleep_gap * 1.5, timeout / 2)
            time.sleep(sleep_gap)
            continue

    raise Exception(f"Error occurred while post to {url}: finally timeout after {timeout}s")



def tcp_cache_get(ip: str, port: int, key: str, *, timeout: float = 5) -> Optional[Any]:
    try:
        sket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sket.settimeout(timeout)
        sket.connect((ip, port))
        sket.sendall(key.encode())

        segs: List[bytes] = []
        while True:
            seg = sket.recv(1024 * 1024)
            if not seg:
                break
            segs.append(seg)

        sket.close()

        reply: Reply = Reply.from_bytes(b''.join(segs))

        if reply.is_msg():
            logging.warning(f"Error occurred while sending TCP cache-get request to {ip}:{port}: {reply.msg}")
            return None
        
        assert(reply.obj is not None)
        return pickle.loads(reply.obj)

    except Exception as e:
        logging.warning(f"Error occurred while sending TCP cache-get request to {ip}:{port}: {str(e)}")
        return None