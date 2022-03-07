import sys
from commitizen.bump import update_version_in_files


def main(cur_vers, new_vers):
    update_version_in_files(
        current_version=cur_vers.lstrip("v"),
        new_version=new_vers.lstrip("v"),
        files=["pyproject.toml", "hpcflow/_version.py"],
    )


if __name__ == "__main__":
    print(sys.argv)
    main(*sys.argv[1:])
