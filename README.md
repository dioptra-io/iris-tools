This repository contains a collection of tools for Iris developers and
maintainers to make it easier to manage Iris and publish its metadata
and data.  Currently it includes tools for:

  - Publishing Iris metadata and data
  - Scanning container logs and checking logged metrics
  - Updating `iris-agent` containers without having to recreate them

Note that some of these tools, which interact directly with the
Iris containers running on the Iris production server, must be
executed directly on the Iris production server.  Additionally, the
`update_agents.sh` tool, which is used to update `iris-agent` containers,
requires an authenticated `gcloud` session.
