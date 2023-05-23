from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from param_load_data_gcs import parent_flow

docker_block = DockerContainer.load("docker-gcs")
docker_dep = Deployment.build_from_flow(
    flow = parent_flow,
    name = 'docker_gcs',
    infrastructure = docker_block
)

if __name__ == "__main__":
    docker_dep.apply()