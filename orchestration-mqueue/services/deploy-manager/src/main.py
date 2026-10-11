from pathlib import Path
from .manifest.parser import ManifestParser
from .manifest.exceptions import ParserError
from .deploy.runner import DockerContainerRunner
from .deploy.exceptions import ContainerRunnerError
from .redisdb.redis_loader import RedisLoader  # NEW
from .redisdb.exceptions import RedisLoaderError
from .exceptions import DeployManagerError
import sys


def main():

    # Init phase 
    mission_path = Path("/tmp")
    # mission_path = Path("/home/vboxuser/projects/RT-microservices-choreography-pe/tests/test_0_code")

    # Parse manifest phase 
    parser = ManifestParser(str(mission_path / "manifest.yaml"))
    try:
        schedule = parser.parse()
        pass
    except ParserError as e:
        print(f"Deploy Manager failed: {e}")
        sys.exit(1)


    # Container Running phase
    docker_runner = DockerContainerRunner()
    try:
        # Run containers for each task
        for task in schedule.tasks:
            print(f"Processing task '{task.image}'")

            # Run container
            container_name = task.image
            docker_runner.run_task_service(
                image_tag=task.image,
                container_name=container_name,
            )
    except ContainerRunnerError as e:
        print(f"Deploy Manager failed: {e}")
        sys.exit(1)
    
    # Redis Loading phase
    redis_loader = RedisLoader(host="redis", port=6379)

    try:
        redis_loader.load_schedule(schedule)
        redis_loader.debug_print()  # REMOVE AFTER DEBUGGING
    except RedisLoaderError as e:
        print(f"Deploy Manager failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
