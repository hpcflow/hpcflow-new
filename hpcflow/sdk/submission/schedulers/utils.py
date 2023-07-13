import subprocess


def run_cmd(cmd):
    """Execute a command and return stdout, stderr as strings."""
    print(f"running shell command: {cmd}")  # TODO log.debug
    proc = subprocess.run(args=cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = proc.stdout.decode()
    stderr = proc.stderr.decode()
    return stdout, stderr
