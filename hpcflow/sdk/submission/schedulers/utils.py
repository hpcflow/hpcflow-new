import subprocess


def run_cmd(cmd, logger=None):
    """Execute a command and return stdout, stderr as strings."""
    if logger:
        logger.debug(f"running shell command: {cmd}")
    proc = subprocess.run(args=cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = proc.stdout.decode()
    stderr = proc.stderr.decode()
    return stdout, stderr
