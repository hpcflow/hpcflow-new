from getpass import getpass
from paramiko.ssh_exception import SSHException

from hpcflow.sdk.core.errors import WorkflowNotFoundError


def ask_pw_on_auth_exc(f, *args, add_pw_to=None, **kwargs):
    try:
        out = f(*args, **kwargs)
        pw = None

    except SSHException:
        pw = getpass()

        if not add_pw_to:
            kwargs["password"] = pw
        else:
            kwargs[add_pw_to]["password"] = pw

        out = f(*args, **kwargs)

        if not add_pw_to:
            del kwargs["password"]
        else:
            del kwargs[add_pw_to]["password"]

    return out, pw


def infer_store(path: str, fs) -> str:
    """Identify the store type using the path and file system parsed by fsspec.

    Parameters
    ----------
    fs
        fsspec file system

    """
    # try to identify store type just from the path string:
    if path.endswith(".zip"):
        store_fmt = "zip"

    elif path.endswith(".json"):
        store_fmt = "json-single"

    else:
        # look at the directory contents:
        if fs.glob(f"{path}/.zattrs"):
            store_fmt = "zarr"
        elif fs.glob(f"{path}/metadata.json"):
            store_fmt = "json"
        else:
            raise WorkflowNotFoundError(
                f"Cannot infer a store format at path {path!r} with file system {fs!r}."
            )

    return store_fmt
