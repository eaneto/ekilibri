import os
import signal
import subprocess
import time
from typing import Optional

import psutil


def setup_command_server(request, port: int) -> int:
    profile = request.config.getoption("--profile")
    return initialize_command_server(profile, port)


def initialize_command_server(
    profile: str, port: int = 8081, args: Optional[str] = None
):
    if profile == "docker":
        binary = "command"
    else:
        binary = "./target/release/command"
    command = [binary, "-p", str(port)]
    if args is not None:
        command.extend(args.split(" "))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(0.1)
    return process.pid


def kill_process(pid):
    if pid != -1:
        os.kill(pid, signal.SIGTERM)
        time.sleep(0.5)


def setup_ekilibri_server(request, config_path: str) -> int:
    profile = request.config.getoption("--profile")
    attach_logs = request.config.getoption("--attach-logs")
    if request.config.getoption("--setup-server") == "true":
        return initialize_ekilibri_server(profile, attach_logs, config_path)
    else:
        return -1


def initialize_ekilibri_server(
    profile: str,
    attach_logs: str,
    config_path: str,
    port: int = 8080,
    args: Optional[str] = None,
):
    if profile == "docker":
        binary = "ekilibri"
    else:
        binary = "./target/release/ekilibri"
    command = [binary, "-f", config_path]
    if args is not None:
        command.extend(args.split(" "))
    if attach_logs == "true":
        process = subprocess.Popen(command)
    else:
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
    time.sleep(0.1)
    return process.pid


def find_and_kill_command_server():
    processes = psutil.process_iter()
    name = "command"
    process = [p for p in processes if name in p.name()][0]
    os.kill(process.pid, signal.SIGTERM)


def find_and_kill_all_command_servers():
    processes = psutil.process_iter()
    name = "command"
    processes = [p for p in processes if name in p.name()]
    for process in processes:
        os.kill(process.pid, signal.SIGTERM)
