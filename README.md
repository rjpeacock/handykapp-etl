# handykapp-etl

[![Skylos Grade](https://img.shields.io/badge/Skylos-A%2B%20%28100%29-brightgreen)](https://github.com/duriantaco/skylos)

ETL pipeline to feed the handykapp horse racing analysis app
Used in conjunction with Prefect orchestration tool

## Updating deployments

Prefect deployments run ETL flows in Docker containers for consistency and portability. Update the project's Docker image when code or dependencies change, then redeploy flows individually using Prefect. This ensures reliable, repeatable deployments and easy rollback if needed.

To update the Docker image and redeploy your Prefect flows:

1. **Run the update script**

- Use the custom script `./docker_update.sh` to build and push the Docker image.

1. **Update the image tag in `prefect.yaml` only if you changed the tag**

- Edit the `job_variables.image` field for the relevant deployment if the tag has changed.

1. **Deploy flows one at a time**

- Use:

   ```bash
   poetry run prefect deploy
   ```

  - Repeat for each flow you want to redeploy.

**Notes:**

- Only update the image reference if you change the tag.
- For troubleshooting, check Prefect agent logs and Docker container logs.
