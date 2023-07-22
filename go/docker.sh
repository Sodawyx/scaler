docker login --username=sodawyx --password wyx990415 registry.cn-beijing.aliyuncs.com
docker buildx build --platform linux/amd64 -t registry.cn-beijing.aliyuncs.com/free4inno/scaler:v1.0 . --push
kubectl apply -f ../manifest/serverless-simulation.yaml