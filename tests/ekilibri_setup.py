import os
import shutil
import signal
import subprocess
import time
from typing import Optional


def setup_ekilibri_server(request) -> int:
    profile = request.config.getoption("--profile")
    return initialize_ekilibri_server(profile)


def initialize_ekilibri_server(
    profile: str, port: int = 7878, args: Optional[str] = None
):
    if profile == "docker":
        binary = "rog-server"
    else:
        binary = "./target/release/ekilibri"
    command = [binary, "-p", str(port)]
    if args is not None:
        command.extend(args.split(" "))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(0.5)
    return process.pid


def kill_ekilibri_server(pid):
    os.kill(pid, signal.SIGTERM)
    time.sleep(0.5)
