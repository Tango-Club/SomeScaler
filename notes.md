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

### 资料
1. 赛题任务提交说明 https://tianchi.aliyun.com/forum/post/558730
2. 赛题 https://tianchi.aliyun.com/competition/entrance/532103/information
3. codebase https://github.com/AliyunContainerService/scaler