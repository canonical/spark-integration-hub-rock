# spark-integration-hub-rock

Charmed Spark is a set of Canonical supported artifacts (including charms, rocks and SNAPs) that makes operating Spark workloads on Kubernetes seamless, secure and production-ready.

This repository contains the packing and metadata for the Spark Integrator Hub Rock image. 
It contains the business logic and component to update and to provide the Spark configuration options to the final user in an automatic way.

## Build

Building requires to have installed `rockcraft`. In Ubuntu, you can install `rockcraft` with

```bash 
snap install rockcraft --classic
```

Then, to build the Spark Integration rock image, simply use

```bash
rockcraft pack
```

This will create a `.rock` file that is an OCI compliant image. To upload the file to your local Docker registry use skopeo that is bundled in the `rockcraft` snap:

```bash
sudo rockcraft.skopeo --insecure-policy copy oci-archive:<file> docker-daemon:<image>:<tag>
```
