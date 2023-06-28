# notes

### 登陆registry
```sh
docker login --username=pxl290 registry.cn-shanghai.aliyuncs.com
```

### 打包
```sh
make binary
docker buildx build --platform linux/amd64 -t registry.cn-shanghai.aliyuncs.com/somescaler/scaler:latest . --push
```
