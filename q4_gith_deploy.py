
from prefect.filesystems import GitHub
from prefect.deployments import Deployment

from q4_etl_web_to_gcs import etl_web_to_gcs


github_block = GitHub.load("gith")

gh_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="gh-flow"
)


if __name__ == "__main__":
    gh_dep.apply()
