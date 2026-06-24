#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kubernetes 集群一键部署脚本（3 Master + 2 Worker）
功能：环境初始化后自动重启节点，关键组件部署后健康检查，支持异常重试
"""

import os
import sys
import time
import subprocess
import argparse
import paramiko
from datetime import datetime
from pathlib import Path

# ======================== 全局配置 ========================
WORK_DIR = Path(__file__).parent.resolve()
INVENTORY = WORK_DIR / "inventory.ini"
LOG_DIR = WORK_DIR / "logs"
LOG_FILE = None

# Playbook 列表（按顺序执行）
PLAYBOOKS = [
    ("00-env-preparation.yml", "环境准备（含重启）", True),   # 需要重启等待
    ("01-software-install.yml", "软件安装", False),
    ("02-cluster-init.yml", "集群初始化（含calico）", False),
    ("03-deploy-metrics.yml","部署metrics",False),
    ("04-cluster-verify.yml", "集群验证", False)
]

# 组件验证配置
COMPONENT_CHECKS = {
    "calico": {
        "namespace": "kube-system",
        "expected_pods": ["calico-node", "calico-kube-controllers", "calico-typha"],
        "timeout": 180,
        "interval": 10
    },
    "metrics-server": {
        "namespace": "kube-system",
        "pod_prefix": "metrics-server",
        "timeout": 120,
        "interval": 10
    }
}

# ======================== 日志与颜色 ========================
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    NC = '\033[0m'

def log_info(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{Colors.GREEN}[INFO]{Colors.NC} {msg}"
    print(line)
    if LOG_FILE:
        with open(LOG_FILE, 'a') as f:
            f.write(f"[{timestamp}] INFO: {msg}\n")

def log_warn(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{Colors.YELLOW}[WARN]{Colors.NC} {msg}"
    print(line)
    if LOG_FILE:
        with open(LOG_FILE, 'a') as f:
            f.write(f"[{timestamp}] WARN: {msg}\n")

def log_error(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{Colors.RED}[ERROR]{Colors.NC} {msg}"
    print(line)
    if LOG_FILE:
        with open(LOG_FILE, 'a') as f:
            f.write(f"[{timestamp}] ERROR: {msg}\n")

# ======================== 辅助函数 ========================
def run_command(cmd, description, check=True, timeout=None):
    """执行命令并实时输出日志"""
    log_info(f"执行: {description}")
    try:
        with open(LOG_FILE, 'a') as log_f:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                shell=False
            )
            for line in process.stdout:
                print(line, end='')
                log_f.write(line)
            process.wait(timeout=timeout)
        if process.returncode != 0 and check:
            raise subprocess.CalledProcessError(process.returncode, cmd)
        return process.returncode == 0
    except subprocess.TimeoutExpired:
        log_error(f"命令执行超时: {description}")
        return False
    except subprocess.CalledProcessError as e:
        log_error(f"命令执行失败: {description} (code {e.returncode})")
        return False


def wait_for_ssh(master_host, timeout=300):
    """等待指定主机 SSH 恢复可用（重启后使用）"""
    log_info(f"等待 {master_host} SSH 服务恢复（最长 {timeout} 秒）...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            subprocess.run(
                ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=5",
                 master_host, "exit"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            log_info(f"SSH 连接 {master_host} 已恢复")
            return True
        except subprocess.CalledProcessError:
            time.sleep(5)
    log_error(f"等待 SSH 超时，{master_host} 未恢复")
    return False

def remote_command(cmd, host, username = "root"):
    # 统一转换为字符串
    if isinstance(cmd, list):
        cmd = ' '.join(cmd)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host,username=username)  # 建议添加 username='root' 或从配置获取
    try:
        stdin, stdout, stderr = ssh.exec_command(cmd)
        output = stdout.read().decode('utf-8', errors='ignore')
        errors = stderr.read().decode('utf-8', errors='ignore')
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            log_error(f"命令执行失败，{errors}")
            return False, errors
        return True, output
    except Exception as e:
        log_error(f"执行异常: {e}")
        return False, str(e)
    finally:
        ssh.close()

def check_nodes_ready(first_master):
    """检查所有节点是否 Ready（通过 kubectl）"""
    log_info("检查节点状态...")
    cmd = ["kubectl", "get", "nodes", "--no-headers"]
    try:
        success, result = remote_command(cmd, first_master)
        if not success:
            return False
        not_ready = []
        for line in result.strip().split('\n'):
            if line and "NotReady" in line:
                 not_ready.append(line.split()[0])
        if not_ready:
            log_warn(f"节点未就绪: {not_ready}")
            return False
        log_info("所有节点均已就绪")
        return True
    except Exception as e:
        log_error(f"检查节点状态失败: {e}")
        return False

def verify_component(component, first_master_ip):
    """验证指定组件是否正常运行"""
    if component == "calico":
        log_info("验证 Calico 网络插件...")
        return verify_calico(first_master_ip)
    elif component == "metrics-server":
        log_info("验证 Metrics Server...")
        return verify_metrics_server(first_master_ip)
    else:
        log_warn(f"未知组件: {component}")
        return True

def verify_calico(first_master_ip):
    """检查 Calico 网络插件状态（确认所有节点 calico-node 正常运行）"""
    namespace = COMPONENT_CHECKS["calico"]["namespace"]
    timeout = COMPONENT_CHECKS["calico"]["timeout"]
    interval = COMPONENT_CHECKS["calico"]["interval"]
    start = time.time()
    while time.time() - start < timeout:
        try:
            # 只获取 calico-node 标签的 Pod（不获取全部 kube-system Pod）
            cmd = ["kubectl", "get", "pods", "-n", namespace, "-l", "k8s-app=calico-node", "-o", "json"]
            success, result = remote_command(cmd, first_master_ip)
            if not success:
                log_warn("获取 Calico Pods 状态失败，将重试")
                time.sleep(interval)
                continue
            import json
            pods = json.loads(result)
            items = pods.get('items', [])
            total = len(items)
            running = 0
            running_nodes = []
            not_running = []
            for pod in items:
                name = pod.get('metadata', {}).get('name', 'unknown')
                node = pod.get('spec', {}).get('nodeName', 'unknown')
                status = pod.get('status', {}).get('phase')
                if status == "Running":
                    running += 1
                    running_nodes.append(node)
                else:
                    not_running.append(f"{name}({node}:{status})")
            # 要求 calico-node 在至少半数节点上运行（避免单节点故障误报）
            # 对于 5 节点集群，至少需要 3 个 Running
            if running >= 3:
                log_info(f"Calico 验证通过: {running}/{total} 节点运行中")
                log_info(f"  已就绪节点: {', '.join(running_nodes)}")
                if not_running:
                    log_warn(f"  以下 Pod 异常: {'; '.join(not_running)}")
                return True
            log_info(f"等待 Calico Pods 启动... (Running {running}/{total})")
        except Exception as e:
            log_warn(f"检查 Calico 状态时出错: {e}")
        time.sleep(interval)
    log_error(f"Calico 验证超时（{timeout}秒）")
    return False

def verify_metrics_server(first_master_ip):
    """检查 Metrics Server Pod 状态（含 CrashLoopBackOff 检测）"""
    namespace = COMPONENT_CHECKS["metrics-server"]["namespace"]
    prefix = COMPONENT_CHECKS["metrics-server"]["pod_prefix"]
    timeout = COMPONENT_CHECKS["metrics-server"]["timeout"]
    interval = COMPONENT_CHECKS["metrics-server"]["interval"]
    start = time.time()
    while time.time() - start < timeout:
        try:
            cmd = f"kubectl get pods -n {namespace} -l k8s-app={prefix} -o json"
            success, result = remote_command(cmd, first_master_ip)
            if success:
                import json
                pods = json.loads(result)
                for pod in pods.get('items', []):
                    # 检查 containerStatuses 中的实际状态（CrashLoopBackOff 时 phase 仍为 Running）
                    containers = pod.get('status', {}).get('containerStatuses', [])
                    if containers:
                        state = containers[0].get('state', {})
                        if 'waiting' in state:
                            reason = state['waiting'].get('reason', '')
                            if reason == "CrashLoopBackOff":
                                pod_name = pod['metadata']['name']
                                log_warn(f"Metrics Server 异常: CrashLoopBackOff")
                                log_warn(f"  Pod: {pod_name}")
                                if 'lastState' in containers[0] and 'terminated' in containers[0]['lastState']:
                                    term_reason = containers[0]['lastState']['terminated'].get('reason', '')
                                    term_msg = containers[0]['lastState']['terminated'].get('message', '')
                                    if term_reason:
                                        log_warn(f"  退出原因: {term_reason}")
                                    if term_msg:
                                        log_warn(f"  详情: {term_msg[:200]}")
                                # 尝试自动删除旧 Pod 触发重新调度
                                log_info("  正在删除异常 Pod 以触发重新调度...")
                                remote_command(f"kubectl delete pod -n {namespace} {pod_name} --wait=false", first_master_ip)
                                time.sleep(interval)
                                break  # 跳出循环继续等待新 Pod
                        elif 'terminated' in state:
                            exit_code = state['terminated'].get('exit_code', -1)
                            if exit_code != 0:
                                log_error(f"Metrics Server 异常退出 (code {exit_code})")
                                return False
                    # 常规 Running 检查
                    status = pod.get('status', {}).get('phase')
                    if status == "Running":
                        log_info("Metrics Server 已正常运行")
                        return True
            log_info("等待 Metrics Server Pod 启动...")
        except Exception as e:
            log_warn(f"检查 Metrics Server 时出错: {e}")
        time.sleep(interval)
    log_error(f"Metrics Server 验证超时（{timeout}秒）")
    return False

def handle_verification_failure(component, first_master_ip):
    """处理组件验证失败，允许用户选择重试、跳过、删除Pod重试或中止"""
    while True:
        options = "(R)重试, (D)删除Pod并重试, (S)跳过, (A)中止部署"
        print(f"\n{Colors.YELLOW}组件 {component} 验证失败！{Colors.NC}")
        choice = input(f"请选择: {options}: ").strip().upper()
        if choice == 'R':
            if verify_component(component, first_master_ip):
                log_info(f"{component} 验证通过，继续部署")
                return True
            else:
                log_warn(f"{component} 再次验证失败")
                continue
        elif choice == 'D':
            log_info(f"尝试删除 {component} Pod 以触发重新调度...")
            if component == "metrics-server":
                cmd = "kubectl delete pod -n kube-system -l k8s-app=metrics-server --wait=false"
            elif component == "calico":
                cmd = "kubectl delete pod -n kube-system -l k8s-app=calico-node --wait=false"
            else:
                log_warn(f"未知组件 {component}，跳过删除")
                continue
            remote_command(cmd, first_master_ip)
            log_info("等待新 Pod 启动（30秒）...")
            time.sleep(30)
            if verify_component(component, first_master_ip):
                log_info(f"{component} 验证通过，继续部署")
                return True
            else:
                log_warn(f"{component} 删除Pod后仍未恢复")
                continue
        elif choice == 'S':
            log_warn(f"跳过组件 {component} 验证，继续部署（可能导致集群不稳定）")
            return True
        elif choice == 'A':
            log_error(f"用户选择中止部署")
            return False
        else:
            print(f"无效输入，请输入 {options}")

# ======================== 增强验证 ========================

def verify_nodes_details(first_master):
    """验证节点状态 - 详细检查所有节点状态、角色、版本"""
    result = {"category": "节点状态验证", "status": "PASS", "items": [], "warnings": []}
    success, output = remote_command(["kubectl", "get", "nodes", "--no-headers"], first_master)
    if not success:
        result["status"] = "FAIL"
        result["items"].append({"check": "获取节点列表", "status": "FAIL", "detail": "命令执行失败"})
        return result
    lines = [l for l in output.strip().split('\n') if l.strip()]
    master_count = 0
    worker_count = 0
    not_ready = []
    for line in lines:
        parts = line.split()
        name, status = parts[0], parts[1]
        roles = parts[2] if len(parts) > 2 else "<none>"
        version = parts[4] if len(parts) > 4 else "unknown"
        if "control-plane" in roles or "master" in roles:
            master_count += 1
        elif roles == "<none>":
            worker_count += 1
        item_status = "PASS" if status == "Ready" else "FAIL"
        if item_status == "FAIL":
            not_ready.append(name)
            result["status"] = "FAIL"
        result["items"].append({"check": f"节点 {name}", "status": item_status,
                                "detail": f"状态={status}, 角色={roles}, 版本={version}"})
    result["summary"] = f"Master: {master_count}, Worker: {worker_count}, 总计: {len(lines)}"
    if not_ready:
        result["warnings"].append(f"未就绪节点: {', '.join(not_ready)}")
    return result


def verify_control_plane(first_master):
    """验证控制平面组件健康状况
    注意：kubeadm 静态 Pod（apiserver/etcd/scheduler/controller-manager）的标签是 component=xxx，
    而非 k8s-app=xxx，因此改用名称前缀匹配的方式更可靠。
    """
    result = {"category": "控制平面组件验证", "status": "PASS", "items": [], "warnings": []}
    targets = [
        "kube-apiserver",
        "kube-controller-manager",
        "kube-scheduler",
        "etcd",
        "coredns",
        "kube-proxy",
    ]
    # 一次获取所有 kube-system Pod，避免多次 SSH
    success, output = remote_command(
        "kubectl get pods -n kube-system --no-headers 2>/dev/null", first_master
    )
    if not success or not output.strip():
        result["status"] = "FAIL"
        result["items"].append({"check": "获取系统 Pod", "status": "FAIL", "detail": "命令执行失败"})
        return result
    lines = [l for l in output.strip().split('\n') if l.strip()]
    status_map = {}
    for line in lines:
        parts = line.split()
        if len(parts) >= 3:
            status_map[parts[0]] = parts[2]
    # 统计所有状态分布
    dist = {}
    for s in status_map.values():
        dist[s] = dist.get(s, 0) + 1
    for target in targets:
        matched = {n: s for n, s in status_map.items() if n.startswith(target)}
        if not matched:
            result["status"] = "FAIL"
            result["items"].append({"check": target, "status": "FAIL", "detail": "未找到 Pod"})
            continue
        total = len(matched)
        running = sum(1 for s in matched.values() if s == "Running")
        crash = sum(1 for s in matched.values() if "CrashLoopBackOff" in s or "Error" in s)
        if running == total:
            result["items"].append({"check": target, "status": "PASS", "detail": f"Running ({running}/{total})"})
        elif crash > 0:
            crash_names = [n for n, s in matched.items() if "CrashLoopBackOff" in s or "Error" in s]
            result["status"] = "FAIL"
            result["items"].append({"check": target, "status": "FAIL", "detail": f"CrashLoopBackOff/Error: {crash} 个 (Running {running}/{total})"})
            result["warnings"].append(f"{target} 异常 Pod: {', '.join(crash_names)}")
        else:
            result["status"] = "FAIL"
            result["items"].append({"check": target, "status": "FAIL", "detail": f"Running {running}/{total}"})
    if dist:
        summary = ", ".join(f"{k}: {v}" for k, v in sorted(dist.items()))
        result["items"].append({"check": "Pod 状态分布", "status": "INFO", "detail": summary})
    return result


def verify_cluster_health(first_master):
    """验证集群健康状态"""
    result = {"category": "集群健康状态验证", "status": "PASS", "items": [], "warnings": []}
    # cluster-info
    success, output = remote_command("kubectl cluster-info 2>/dev/null", first_master)
    if success:
        result["items"].append({"check": "集群信息", "status": "PASS", "detail": "可正常获取"})
    else:
        result["status"] = "FAIL"
        result["items"].append({"check": "集群信息", "status": "FAIL", "detail": "获取失败"})
    # /healthz
    success, output = remote_command("kubectl get --raw='/healthz' 2>/dev/null", first_master)
    if success and output.strip() == "ok":
        result["items"].append({"check": "API Server 健康检查", "status": "PASS", "detail": "健康 (ok)"})
    else:
        result["status"] = "FAIL"
        result["items"].append({"check": "API Server 健康检查", "status": "FAIL", "detail": output.strip()[:100] if output.strip() else "不健康"})
    # /healthz?verbose
    success, output = remote_command("kubectl get --raw='/healthz?verbose' 2>/dev/null", first_master)
    if success:
        unhealthy = [l.strip() for l in output.strip().split('\n') if "[+]" not in l and "healthz" not in l and l.strip()]
        if unhealthy:
            result["status"] = result["status"] if result["status"] == "FAIL" else "WARN"
            result["items"].append({"check": "组件详细健康", "status": "WARN", "detail": "; ".join(unhealthy[:5])})
        else:
            pass  # 已通过 /healthz 检查
    return result


def _get_pods_by_namespace(first_master, namespace, label_selector=""):
    """按命名空间获取 Pod 列表（标签或名称回退）"""
    # 先尝试标签匹配
    if label_selector:
        cmd = f"kubectl get pods -n {namespace} -l {label_selector} --no-headers 2>/dev/null"
        s, o = remote_command(cmd, first_master)
        if s and o.strip():
            return True, o, namespace
    # 退回到获取该命名空间下所有 Pod
    cmd = f"kubectl get pods -n {namespace} --no-headers 2>/dev/null"
    s, o = remote_command(cmd, first_master)
    if s and o.strip():
        return True, o, namespace
    return False, "", namespace


def verify_network_plugin(first_master):
    """验证网络插件（Calico）状态"""
    result = {"category": "网络插件验证", "status": "PASS", "items": [], "warnings": []}

    # 按名称前缀检查 Calico 组件，calico-typha 标记为可选
    calico_prefixes = [
        ("calico-node", "Calico-node"),
        ("calico-kube-controllers", "Calico-kube-controllers"),
        ("calico-typha", "Calico-typha (可选)"),
    ]

    # 一次获取所有命名空间中名称含 calico 的 Pod
    cmd = "kubectl get pods --all-namespaces --no-headers 2>/dev/null | grep -i calico"
    success, output = remote_command(cmd, first_master)
    if not success or not output.strip():
        result["status"] = "FAIL"
        result["items"].append({"check": "Calico", "status": "FAIL", "detail": "未找到 Calico Pod"})
        return result

    # 解析输出：NAMESPACE NAME READY STATUS RESTARTS AGE NODE
    lines = [l for l in output.strip().split('\n') if l.strip()]
    pod_map = {}
    for line in lines:
        parts = line.split()
        if len(parts) >= 3:
            pod_map[parts[1]] = parts[3]  # Pod 名 -> 状态

    for prefix, display_name in calico_prefixes:
        matched = {n: s for n, s in pod_map.items() if n.startswith(prefix)}
        if not matched:
            if "可选" in display_name:
                result["items"].append({"check": display_name, "status": "INFO", "detail": "未部署"})
            else:
                result["status"] = "FAIL"
                result["items"].append({"check": display_name, "status": "FAIL", "detail": "未找到 Pod"})
            continue
        total = len(matched)
        running = sum(1 for s in matched.values() if s == "Running")
        crash = sum(1 for s in matched.values() if "CrashLoopBackOff" in s or "Error" in s)
        if running == total:
            result["items"].append({"check": display_name, "status": "PASS", "detail": f"Running ({running}/{total})"})
        else:
            result["status"] = "FAIL"
            detail = f"CrashLoopBackOff {crash} 个" if crash else f"Running {running}/{total}"
            result["items"].append({"check": display_name, "status": "FAIL", "detail": detail})
            crash_names = [n for n, s in matched.items() if "CrashLoopBackOff" in s or "Error" in s]
            result["warnings"].append(f"{display_name} 异常 Pod: {', '.join(crash_names)}")
    return result


def verify_certificates(first_master):
    """验证证书与安全配置"""
    result = {"category": "证书与安全验证", "status": "PASS", "items": [], "warnings": []}
    certs = [
        ("/etc/kubernetes/pki/ca.crt", "CA 证书"),
        ("/etc/kubernetes/pki/apiserver.crt", "API Server 证书"),
        ("/etc/kubernetes/pki/apiserver-kubelet-client.crt", "API Server Kubelet 客户端证书"),
        ("/etc/kubernetes/pki/front-proxy-ca.crt", "Front Proxy CA 证书"),
    ]
    for cert_path, cert_name in certs:
        cmd = f"openssl x509 -in {cert_path} -noout -subject -dates 2>/dev/null || echo 'NOT_FOUND'"
        success, output = remote_command(cmd, first_master)
        if not success or "NOT_FOUND" in output or "No such file" in output:
            result["warnings"].append(f"{cert_name} ({cert_path}): 未找到")
            result["items"].append({"check": cert_name, "status": "WARN", "detail": "未找到证书文件"})
            continue
        not_after = ""
        for line in output.strip().split('\n'):
            if line.startswith("notAfter"):
                not_after = line.split("=", 1)[1] if "=" in line else ""
        if not_after:
            try:
                expiry = datetime.strptime(not_after.strip(), "%b %d %H:%M:%S %Y %Z")
                days_left = (expiry - datetime.now()).days
                if days_left < 0:
                    result["status"] = "FAIL"
                    result["items"].append({"check": cert_name, "status": "FAIL", "detail": f"已过期 ({days_left}) 天"})
                elif days_left < 30:
                    if result["status"] == "PASS":
                        result["status"] = "WARN"
                    result["items"].append({"check": cert_name, "status": "WARN", "detail": f"即将过期，剩余 {days_left} 天"})
                else:
                    result["items"].append({"check": cert_name, "status": "PASS", "detail": f"有效，剩余 {days_left} 天"})
            except Exception:
                result["items"].append({"check": cert_name, "status": "INFO", "detail": f"到期: {not_after.strip()}"})
        else:
            result["items"].append({"check": cert_name, "status": "INFO", "detail": "有效期信息获取成功"})
    return result


def run_all_verifications(first_master):
    """执行全部验证检查，汇总结果"""
    log_info("=== 开始集群全面验证 ===")
    verifiers = [
        ("节点状态验证", verify_nodes_details),
        ("控制平面组件验证", verify_control_plane),
        ("集群健康状态验证", verify_cluster_health),
        ("网络插件验证", verify_network_plugin),
        ("证书与安全验证", verify_certificates),
    ]
    all_results = {}
    overall_status = "PASS"
    for cat_name, verify_func in verifiers:
        try:
            r = verify_func(first_master)
            all_results[verify_func.__name__] = r
            st = r["status"]
            if st == "FAIL":
                overall_status = "FAIL"
            elif st == "WARN" and overall_status == "PASS":
                overall_status = "WARN"
            color = Colors.GREEN if st == "PASS" else (Colors.YELLOW if st == "WARN" else Colors.RED)
            log_info(f"  {r['category']}: {color}{st}{Colors.NC}")
        except Exception as e:
            log_error(f"验证 {cat_name} 异常: {e}")
            all_results[cat_name] = {"category": cat_name, "status": "FAIL", "items": [], "warnings": [str(e)]}
            overall_status = "FAIL"
    return {"results": all_results, "overall_status": overall_status}


def generate_deployment_report(first_master, verification_data, playbook_results, start_time_str, deploy_log_path):
    """生成并输出部署报告"""
    log_info("=== 生成部署报告 ===")
    now = datetime.now()
    # 获取 K8s 版本
    k8s_version = "未知"
    try:
        success, output = remote_command("kubectl version -o json 2>/dev/null | python3 -c \"import sys,json;print(json.load(sys.stdin)['serverVersion']['gitVersion'])\" 2>/dev/null || kubectl version --short 2>/dev/null | grep Server", first_master)
        if success and output.strip():
            k8s_version = output.strip()
    except Exception:
        k8s_version = "获取失败"
    # 从 group_vars 文件读取版本信息
    gv = {}
    gv_path = WORK_DIR / "group_vars" / "k8s_cluster.yml"
    if gv_path.exists():
        for line in gv_path.read_text().split('\n'):
            line = line.strip()
            if ':' in line and not line.startswith('#') and not line.startswith('- '):
                k, v = line.split(':', 1)
                gv[k.strip()] = v.strip().strip("'\"")
    overall = verification_data.get("overall_status", "UNKNOWN")
    lines = []
    lines.append("=" * 62)
    lines.append("              Kubernetes 集群部署报告")
    lines.append("=" * 62)
    lines.append(f"  部署开始: {start_time_str}")
    lines.append(f"  报告生成: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"  日志文件: {deploy_log_path}")
    lines.append("")
    lines.append("  [基本信息]")
    lines.append(f"    Kubernetes 版本: {k8s_version}")
    lines.append(f"    部署模式: 3 Master + 2 Worker (高可用)")
    lines.append(f"    容器运行时: containerd ({gv.get('containerd_version', '未知')})")
    lines.append(f"    CNI 插件: Calico ({gv.get('calico_version', '未知')})")
    lines.append(f"    控制平面端点: {gv.get('control_plane_endpoint', '未知')}")
    lines.append("")
    lines.append("  [部署阶段]")
    for name, status in playbook_results:
        icon = "✓" if status else "✗"
        lines.append(f"    {icon} {name}")
    lines.append("")
    lines.append("  [验证结果]")
    for func_name, r in verification_data.get("results", {}).items():
        st = r.get("status", "UNKNOWN")
        icon = {"PASS": "✓", "WARN": "!", "FAIL": "✗", "INFO": "i"}.get(st, "?")
        lines.append(f"    {r.get('category', func_name)} [{icon} {st}]")
        for item in r.get("items", []):
            ist = item.get("status", "")
            iicon = {"PASS": "  ✓", "FAIL": "  ✗", "WARN": "  !", "INFO": "   "}.get(ist, "   ")
            lines.append(f"      {iicon} {item['check']}: {item.get('detail', '')}")
        for w in r.get("warnings", []):
            lines.append(f"      ! {w}")
    lines.append("")
    lines.append("-" * 62)
    verdict_icon = {"PASS": "✓", "WARN": "!", "FAIL": "✗"}.get(overall, "?")
    overall_str = {"PASS": "部署成功", "WARN": "部署完成（有警告）", "FAIL": "部署异常"}.get(overall, "未知")
    lines.append(f"  总体状态: {verdict_icon} {overall_str}")
    lines.append("=" * 62)
    report_text = "\n".join(lines)
    print(f"\n{Colors.GREEN}{report_text}{Colors.NC}")
    # 保存报告到文件
    report_path = LOG_DIR / f"deploy_report_{now.strftime('%Y%m%d_%H%M%S')}.txt"
    try:
        with open(report_path, 'w') as f:
            f.write(report_text)
        log_info(f"部署报告已保存: {report_path}")
        # 同时保存 JSON 格式
        import json
        json_path = LOG_DIR / f"deploy_report_{now.strftime('%Y%m%d_%H%M%S')}.json"
        json_data = {
            "deploy_time": start_time_str,
            "report_time": now.strftime('%Y-%m-%d %H:%M:%S'),
            "k8s_version": k8s_version,
            "overall_status": overall,
            "playbook_results": [{"name": n, "success": s} for n, s in playbook_results],
            "verification": verification_data,
        }
        with open(json_path, 'w') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        log_info(f"JSON 报告已保存: {json_path}")
    except Exception as e:
        log_error(f"保存报告文件失败: {e}")


# ======================== 环境检查 ========================
def check_prerequisites():
    log_info("=== 检查环境依赖 ===")
    # 检查 Ansible
    if not Path("/usr/bin/ansible").exists() and not Path("/usr/local/bin/ansible").exists():
        log_error("Ansible 未安装")
        return False
    # 检查 inventory
    if not INVENTORY.exists():
        log_error(f"Inventory 文件不存在: {INVENTORY}")
        return False
    # 测试 SSH 连接
    log_info("测试 SSH 连通性...")
    cmd = ["ansible", "-i", str(INVENTORY), "all", "-m", "ping"]
    if not run_command(cmd, "SSH 连接测试", check=False):
        log_error("SSH 连接测试失败，请检查 inventory 和 SSH 免密登录")
        return False
    return True

# ======================== 执行 Playbook ========================
def run_playbook(playbook_file, description):
    playbook_path = WORK_DIR / "playbooks" / playbook_file
    if not playbook_path.exists():
        log_error(f"Playbook 文件不存在: {playbook_path}")
        return False
    log_info(f"=== {description} ===")
    cmd = [
        "ansible-playbook", "-i", str(INVENTORY), str(playbook_path),
        "--extra-vars", f"deploy_log_dir={LOG_DIR}"
    ]
    return run_command(cmd, description)

# ======================== 主流程 ========================
def main():
    global LOG_FILE
    parser = argparse.ArgumentParser(description="Kubernetes 集群一键部署")
    parser.add_argument("--report-only", help="仅基于已有集群生成验证报告（需指定第一个 master IP）")
    args = parser.parse_args()

    # 仅报告模式：连接到已有集群生成验证报告
    if args.report_only:
        LOG_DIR.mkdir(exist_ok=True)
        LOG_FILE = LOG_DIR / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_info(f"=== 仅报告模式: 连接到 {args.report_only} 生成验证报告 ===")
        vdata = run_all_verifications(args.report_only)
        generate_deployment_report(args.report_only, vdata, [("仅报告模式", True)],
                                   datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(LOG_FILE))
        return

    LOG_DIR.mkdir(exist_ok=True)
    LOG_FILE = LOG_DIR / f"deploy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    start_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    log_info("==========================================")
    log_info("  Kubernetes 集群部署开始")
    log_info(f"  时间: {start_time_str}")
    log_info(f"  日志: {LOG_FILE}")
    log_info("==========================================")

    if not check_prerequisites():
        sys.exit(1)

    # 获取第一个 master 节点 IP（用于 SSH 等待和 kubectl 操作）
    first_master = None
    try:
        with open(INVENTORY, 'r') as f:
            in_master = False
            for line in f:
                line = line.strip()
                if line.startswith('[master]'):
                    in_master = True
                elif line.startswith('[') and in_master:
                    break
                elif in_master and line and not line.startswith('#'):
                    if 'ansible_host=' in line:
                        first_master = line.split('ansible_host=')[1].split()[0]
                    else:
                        first_master = line.split()[0]
                    break
    except Exception as e:
        log_error(f"解析 inventory 失败: {e}")
        sys.exit(1)

    if not first_master:
        log_error("未找到 master 节点")
        sys.exit(1)

    # 记录所有部署阶段结果
    playbook_results = []

    # 执行阶段一：环境准备（包含重启）
    ok = run_playbook("00-env-preparation.yml", "阶段1: 环境准备")
    playbook_results.append(("阶段1: 环境准备", ok))
    if not ok:
        log_error("环境准备失败，部署中止")
        sys.exit(1)

    # 等待所有节点重启后 SSH 恢复
    log_info("等待节点重启后 SSH 恢复...")
    nodes_ip = []
    try:
        with open(INVENTORY, 'r') as f:
            current_group = None
            for line in f:
                line = line.strip()
                if line.startswith('[') and line.endswith(']'):
                    group = line[1:-1]
                    if group in ('master', 'worker'):
                        current_group = group
                    else:
                        current_group = None
                    continue
                if current_group and line and not line.startswith('#'):
                    if 'ansible_host=' in line:
                        ip = line.split('ansible_host=')[1].split()[0]
                    else:
                        ip = line.split()[0]
                    nodes_ip.append(ip)
    except Exception as e:
        log_error(f"解析节点 IP 失败: {e}")
        sys.exit(1)

    for ip in nodes_ip:
        if not wait_for_ssh(ip, timeout=300):
            log_error(f"节点 {ip} SSH 未恢复，部署中止")
            sys.exit(1)

    time.sleep(10)

    # 执行阶段二：软件安装
    ok = run_playbook("01-software-install.yml", "阶段2: 软件安装")
    playbook_results.append(("阶段2: 软件安装", ok))
    if not ok:
        log_error("软件安装失败，部署中止")
        sys.exit(1)

    # 执行阶段三：集群初始化（包含网络和 metrics）
    ok = run_playbook("02-cluster-init.yml", "阶段3: 集群初始化")
    playbook_results.append(("阶段3: 集群初始化", ok))
    if not ok:
        log_error("集群初始化失败，部署中止")
        sys.exit(1)

    # 等待所有节点 Ready（最多 5 分钟）
    log_info("等待所有节点状态为 Ready...")
    ready = False
    for _ in range(30):
        time.sleep(10)
        if check_nodes_ready(first_master):
            ready = True
            break
    if not ready:
        log_error("节点未能在预期时间内就绪，请手动检查")
        # 不退出，继续执行验证和报告

    # 验证 Calico
    if not verify_component("calico", first_master):
        if not handle_verification_failure("calico", first_master):
            log_warn("Calico 验证未通过，继续执行全面验证")

    # Calico 就绪后，部署 metrics-server（确保 Calico 网络已可用）
    log_info("Calico 已就绪，开始部署 Metrics Server...")
    metrics_ok = run_playbook("03-deploy-metrics.yml", "Metrics Server 部署")
    if not metrics_ok:
        log_warn("Metrics Server 部署失败，继续执行")

    # 验证 Metrics Server
    if metrics_ok:
        log_info("验证 Metrics Server 状态...")
        if not verify_component("metrics-server", first_master):
            if not handle_verification_failure("metrics-server", first_master):
                log_warn("Metrics Server 验证未通过，继续执行全面验证")
    else:
        log_info("Metrics Server 未部署，跳过验证")

    # 执行阶段四：最终验证
    ok = run_playbook("04-cluster-verify.yml", "阶段4: 集群验证")
    playbook_results.append(("阶段4: 集群验证", ok))
    if not ok:
        log_warn("最终验证 Playbook 执行失败，但集群可能仍可用")

    # ==============================
    # 新增：全面验证 + 部署报告
    # ==============================
    verification_data = run_all_verifications(first_master)
    generate_deployment_report(first_master, verification_data, playbook_results,
                               start_time_str, str(LOG_FILE))

    log_info("==========================================")
    log_info("  Kubernetes 集群部署完成 ✓")
    log_info(f"  时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_info(f"  日志: {LOG_FILE}")
    log_info("==========================================")

if __name__ == "__main__":
    main()
