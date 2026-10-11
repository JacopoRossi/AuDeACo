import docker
import os
import sys

class Publisher:
    def __init__(self):
        try:
            self.client = docker.from_env()
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Docker: {e}")

    def publish(self, repository_url, local_image_name):
        """
        local_image_name: the alias used in build (e.g., 'sum')
        docker_user: your DockerHub username
        """
        # DockerHub format: username/repository:tag
        remote_tag = f"{repository_url}:{local_image_name}"
        
        try:
            # 1. Find the local image and tag it for DockerHub
            image = self.client.images.get(local_image_name)
            image.tag(remote_tag)
            print(f"Tagged {local_image_name} as {remote_tag}")

            # 2. Push to DockerHub
            print(f"Pushing to DockerHub...")
            # We don't specify a registry URL here because DockerHub is the default
            for line in self.client.images.push(remote_tag, stream=True, decode=True):
                if 'status' in line:
                    print(f"[{local_image_name}] {line['status']}")
                if 'error' in line:
                    raise Exception(f"Push error: {line['error']}")
            
            print(f"Successfully published to DockerHub: {remote_tag}")
            
        except Exception as e:
            print(f"Failed to publish {local_image_name}: {e}", file=sys.stderr)
            raise