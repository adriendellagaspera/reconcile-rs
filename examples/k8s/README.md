# Kubernetes example

This directory contains an example node plus Kubernetes manifests.

| path | purpose |
|---|---|
| main.rs | env-driven reconcile node |
| Dockerfile | example image |
| base/ | Service, ConfigMap, StatefulSet, Secret template |
| kind/ | local kind overlay and scripts |

The node discovers peers through the headless Service, exposes metrics and readiness endpoints, and
uses a shared cluster key.

Local run:

~~~sh
./examples/k8s/kind/up.sh
./examples/k8s/kind/down.sh
~~~

Production deployments should replace the image reference and provide the cluster key through their
secret-management system.
