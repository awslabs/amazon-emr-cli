import re
import docker
from typing


class EMRLocalDev ():
    
    def start_local_dev(
        container_name: str,
        spark_ui_port: int,
        jupyter_port: int
    ) :
    
        client = docker.from_env()
    
        #TODO
        #add volume mount

        jupyter_port = jupyter_port
        spark_ui_port = spark_ui_port

        container = client.containers.run(container_name, ports={'4040/tcp': spark_ui_port, '8888/tcp': jupyter_port}, detach=True)

        logs = container.logs(stream=True)

        while True:
            log = logs.__next__().decode('utf-8')

            matcher = re.search(r'(?<=token=)[A-Fa-f0-9]{48}', log)

            if matcher:
                token = matcher.group(0)

                print(f'http://127.0.0.1:{jupyter_port}/lab?token={token}')
                print(f'The notebook server token is: {token}')

                break