import docker

class DockerWrapper():
    """Interaction with docker containers using the docker engine API. This requires access to the docker socket under /var/run/docker.sock.
    For this to work mount a volume containing docker.sock in docker-compose. 
    """

    def __init__(self):
        pass

    @property
    def client(self):
        return docker.from_env()

    def pause_container(self, container_name):
        container = self.client.containers.get(container_name)
        container.pause()

    def start_container(self, container_name):
        container = self.client.containers.get(container_name)
        container.start()

    def unpause_container(self, container_name):
        container = self.client.containers.get(container_name)
        container.unpause()

    def restart_container(self, container_name):
        container = self.client.containers.get(container_name)
        container.restart()

    def list_containers(self):
        return [{'name': c.name, 'status': c.status} for c in self.client.containers.list()]

    def restart_container(self, container_name):
        return self.client.containers.get(container_name).status

    def container_status(self, container_name):
        return self.client.containers.get(container_name).status
