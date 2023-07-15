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


### 思路
1. 预测下一次assign的时间。
设之前assign为[t1,t2,t3,t4,t5],每次差距在[d1,d2,d3,d4],求他们的中位数D，下一次请求时间为last+D。
2. 预测请求占用时间。
[q1,q2,q3,q4,q5],中位数Q。
3. 根据D和Q计算窗口大小。
请求的开始到结束，会有多少个请求，就放多少个slot。
也就是Q/D+1
4. 根据D和Q计算idle时间。
暂时固定