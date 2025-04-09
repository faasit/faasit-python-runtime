import requests


def generate(nums):
    for i in range(nums):
        # 生成8192个字节的随机数据
        yield bytes([i % 256] * 8192)

try:
    resp = requests.post("http://localhost:8080", data=generate(50), headers={'Content-Type': 'application/octet-stream'}, stream=True)
    print(resp.content)
except requests.exceptions.RequestException as e:
    print(f"Error occurred while post to url: {str(e)}")
    raise e