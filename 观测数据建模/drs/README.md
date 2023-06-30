# drs

Resource and return system(Double-R-System)  
对可用资源（resource）进行合理规划、拆解和分配，以取得期望的多方面收益（return）

# 代码结构
- main.py：资源分配入口
- policy.py: 策略系统
- data: 数据获取模块
- jobs: 数梦任务
- models: 预估模型
- notebooks: 数据分析 notebook
- solvers: 运筹求解模块

# 离线测试
1. 登录到 luban 开发机
2. `git clone drs.git`
3. `sh test.sh 2`

# 部署上线

#  其他
- 按照[资源分配开发规范](https://cooper.didichuxing.com/knowledge/2199416545872/2199870808002)进行开发
- 统一代码格式化命令：`yapf -i -r -vv --style='{column_limit=100}' .`
