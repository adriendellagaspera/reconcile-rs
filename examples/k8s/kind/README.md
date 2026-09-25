# Local kind cluster

Requirements: Docker, kind, kubectl, and openssl.

From the repository root:

~~~sh
./examples/k8s/kind/up.sh
./examples/k8s/kind/down.sh
~~~

up.sh creates the cluster, builds and loads the example image, creates a shared cluster key, applies
the kustomize overlay, and waits for readiness.

Useful commands:

~~~sh
kubectl get pods -o wide
kubectl logs reconcile-0 -f
kubectl scale statefulset/reconcile --replicas=7
kubectl port-forward pod/reconcile-0 9000:9000
~~~
