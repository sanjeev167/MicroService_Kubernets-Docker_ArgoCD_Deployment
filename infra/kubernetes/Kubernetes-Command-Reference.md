Kubernetes Command Reference (MSIMPL)

I recommend keeping this as Kubernetes-Command-Reference.md. It is organized according to the deployment lifecycle rather than alphabetically, which makes it much easier to remember.

=============================================================================
1. Cluster Verification
=============================================================================
Verify Cluster
kubectl cluster-info

Shows:

Kubernetes API Server
CoreDNS
Verify Nodes
kubectl get nodes

Shows all cluster nodes.

Detailed Node Information
kubectl describe node docker-desktop
Kubernetes Version
kubectl version
=============================================================================
2. Namespace
=============================================================================
Create Namespace
kubectl apply -f namespace.yaml
View Namespace
kubectl get namespaces
Describe Namespace
kubectl describe namespace msimpl
=============================================================================
3. ConfigMaps
=============================================================================
Apply ConfigMap
kubectl apply -f application-config.yaml
View ConfigMaps
kubectl get configmap -n msimpl
Describe One ConfigMap
kubectl describe configmap postgres-config -n msimpl
Describe All ConfigMaps
kubectl describe configmap -n msimpl
=============================================================================
4. Secrets
=============================================================================
Apply Secret
kubectl apply -f postgres-secret.yaml
View Secrets
kubectl get secrets -n msimpl
Describe Secret
kubectl describe secret postgres-secret -n msimpl
=============================================================================
5. Deployments
=============================================================================
Apply Deployment
kubectl apply -f deployment.yaml
View Deployments
kubectl get deployment -n msimpl
View Wide
kubectl get deployment -n msimpl -o wide
Describe Deployment
kubectl describe deployment ms-wallet -n msimpl
Describe All Deployments
kubectl describe deployment -n msimpl
Restart Deployment
kubectl rollout restart deployment ms-wallet -n msimpl
Restart All Services
kubectl rollout restart deployment ms-wallet -n msimpl

kubectl rollout restart deployment ms-notification -n msimpl

kubectl rollout restart deployment ms-transaction-orchestrator -n msimpl
Rollout Status
kubectl rollout status deployment ms-wallet -n msimpl
Rollout History
kubectl rollout history deployment ms-wallet -n msimpl
=============================================================================
6. Pods
=============================================================================
View Pods
kubectl get pods -n msimpl
Wide Output
kubectl get pods -n msimpl -o wide
Describe Pod
kubectl describe pod <pod-name> -n msimpl
Describe All Pods
kubectl describe pod -n msimpl
Delete Pod
kubectl delete pod <pod-name> -n msimpl

Deployment automatically recreates it.

Delete All Pods
kubectl delete pod --all -n msimpl
Execute Inside Pod
kubectl exec -it <pod-name> -n msimpl -- sh
=============================================================================
7. Services
=============================================================================
Apply Service
kubectl apply -f service.yaml
View Services
kubectl get svc -n msimpl
Wide
kubectl get svc -o wide -n msimpl
Describe One Service
kubectl describe svc ms-wallet -n msimpl
Describe All Services
kubectl describe svc -n msimpl
View Endpoints
kubectl get endpoints -n msimpl
=============================================================================
8. Logs
=============================================================================
Deployment Logs
kubectl logs deployment/ms-wallet -n msimpl
Live Logs
kubectl logs -f deployment/ms-wallet -n msimpl
Pod Logs
kubectl logs <pod-name> -n msimpl
Previous Logs
kubectl logs --previous <pod-name> -n msimpl
=============================================================================
9. Port Forward
=============================================================================
Wallet
kubectl port-forward svc/ms-wallet 8080:8080 -n msimpl
Notification
kubectl port-forward svc/ms-notification 8081:8081 -n msimpl
Transaction
kubectl port-forward svc/ms-transaction-orchestrator 8082:8082 -n msimpl
=============================================================================
10. Ingress
=============================================================================
Apply Ingress
kubectl apply -f ingress.yaml
View Ingress
kubectl get ingress -n msimpl
Describe Ingress
kubectl describe ingress msimpl-ingress -n msimpl
Describe All Ingress
kubectl describe ingress -n msimpl
=============================================================================
11. Helm
=============================================================================
//Install nginx
winget Helm.Helm

Version
helm version
Installed Releases
helm list -A
Add Repository
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
Update Repository
helm repo update
Search Repository
helm search repo ingress-nginx
Install NGINX Ingress
helm install ingress-nginx ingress-nginx/ingress-nginx \
--namespace ingress-nginx \
--create-namespace
=============================================================================
12. Ingress Controller
=============================================================================
View Pods
kubectl get pods -n ingress-nginx
View Services
kubectl get svc -n ingress-nginx
View All
kubectl get all -n ingress-nginx
Logs
kubectl logs -f deployment/ingress-nginx-controller -n ingress-nginx
=============================================================================
13. Everything in Namespace
=============================================================================
View Everything
kubectl get all -n msimpl
View All Objects
kubectl get all,configmap,secret,ingress -n msimpl
Describe Everything (by resource type)
kubectl describe deployment -n msimpl

kubectl describe pod -n msimpl

kubectl describe svc -n msimpl

kubectl describe configmap -n msimpl

kubectl describe secret -n msimpl

kubectl describe ingress -n msimpl
=============================================================================
14. Image Updates
=============================================================================

After rebuilding Docker images:

docker build -t ms-wallet:latest .

Restart Deployment:

kubectl rollout restart deployment ms-wallet -n msimpl

Verify:

kubectl rollout status deployment ms-wallet -n msimpl
=============================================================================
15. Cleanup
=============================================================================

Delete One Deployment

kubectl delete deployment ms-wallet -n msimpl

Delete One Service

kubectl delete svc ms-wallet -n msimpl

Delete Everything

kubectl delete all --all -n msimpl

Delete Namespace

kubectl delete namespace msimpl