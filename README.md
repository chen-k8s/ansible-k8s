# 项目名称
ansible+python脚本实现k8s集群自动化部署。
## 项目简介
本项目架构如下：
3个master主节点（堆叠etcd，高可用控制平台）+2个worker节点，runtime用的是containerd，其他组件包括：Calico、CoreDNS、Metrics-server。
## 环境情况
rockyLinux 9 + Ansible[core 2.14.18] + kubernetes 1.31
具体部署情况查看logs/deploy_report
## 使用步骤
# 克隆仓库
git clone https://github.com/chen-k8s/ansible-k8s.git
# 进入目录
cd ansible-k8s
需根据实际情况修改inventory.ini主机清单，修改tools/ssh_login.yaml文件实现免密登录目标主机。
# 执行 Ansible 剧本
ansible-playbook -i inventory.ini tools/ssh_login.yaml
复制ssh公钥到目标主机实现免密登录
./start.py
执行脚本，开始初始化环境、安装集群，最后会进行验证，输出报告。
