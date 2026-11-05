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

## Application index

The live application index uses Canonical's Vanilla Framework 4.30.0, with
responsive tables, Ubuntu typography and accessible links to driver UIs.
It styles the Hub's index only; each driver's native Spark UI remains unchanged.
Refresh the index to discover newly created application routes.
Page markup lives in `app/index.html`; `ui_server.py` fills its standard-library
`string.Template` placeholders with escaped values and rendered route rows.
The template is loaded at startup and included by the `hub-app` packaging part.

CSS and fonts are bundled in `app/static` and served locally,
including behind an ingress path prefix. There are no runtime CDN requests.
Unused Vanilla decorative background images are excluded; embedded icons remain.
OAuth2 identity checks also apply to these asset requests when authentication is
enabled. Third-party licenses and asset provenance are included in that directory.
The existing `hub-app` packaging part includes the assets automatically.
Rebuild and deploy the rock to make index changes available.