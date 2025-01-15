

This repository contains a collection of tools for Iris maintainers
to make it easier to manage Iris.  Currently it includes tools for:

  - Publishing Iris data
  - Scanning container logs
  - Updating `iris-agent` containers without having to recreate them

Note that some of these tools, which interact directly with the
Iris containers running on the Iris server, must be executed on the
Iris server itself.

Also, the tool for updating `iris-agent` containers requires `gcloud`.
