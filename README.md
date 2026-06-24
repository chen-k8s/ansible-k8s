# 项目名称
ansible+python脚本实现k8s集群自动化部署。
## 项目简介
- 本项目架构如下：
- 3个master主节点（堆叠etcd，高可用控制平台）+2个worker节点，runtime用的是containerd，其他组件包括：Calico、CoreDNS、Metrics-server、haproxy。
- 安装过程大致如下：
- 基本环境配置——>安装必要软件、依赖——>安装高可用组件、runtime——>安装集群组件——>初始化集群——>master、worker节点加入集群——>安装Calico、Metrics-server——>验证集群安装情况，输出报告。
## 环境情况
- rockyLinux 9 + Ansible[core 2.14.18] + kubernetes 1.31
- 具体部署情况查看logs/deploy_report
## 使用步骤
### 克隆仓库
git clone https://github.com/chen-k8s/ansible-k8s.git
### 进入目录
- cd ansible-k8s
- 需根据实际情况修改inventory.ini主机清单，修改tools/ssh_login.yaml文件实现免密登录目标主机。
### 执行 Ansible 剧本实现免密登录
ansible-playbook -i inventory.ini tools/ssh_login.yaml
### 执行Python脚本
./start.py
### 备注
本项目供学习交流使用，如果安装集群过程中出现问题，可运行ansible-playbook -i inventory.ini tools/cleanup-k8s.yml清理集群，正常情况下应该不会出问题。
