import tcpfs_client
from threading import Thread
import time

host="127.0.0.1"
port=8888
timings = []

with open("13MbFile", 'rb') as f:
    file_data = f.read()


def upload(key, bucket_id="00c1c8b1-9ddf-4f11-b486-97b866beb6d9"):
    pass
    #upload_request = tcpfs_client.UploadRequest(key, file_data, bucket_id)
    #start = time.time()
    #tcpfs_client.send_upload_request(host, port, upload_request)
    #timings.append(time.time()-start)


def main(thread_count=100):
    threads = []
    for tid in range(thread_count):
        t = Thread(target=upload, args=(f"key-{tid}",))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    print(f"avg: {round(sum(timings)/len(timings))}")

if __name__ == "__main__":
    main()

