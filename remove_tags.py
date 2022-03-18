from subprocess import run, PIPE

# get all tags:
proc = run("git tag", shell=True, stdin=PIPE, stdout=PIPE)
# proc = run("git ls-remote --tags origin", shell=True, stdin=PIPE, stdout=PIPE)
tags = proc.stdout.decode().strip().split("\n")

for tag in tags:

    # tag = tag.split("/")[-1] # if remote
    # print(tag)
    try:
        tagNums = tag.split("v")[1]
    except Exception:
        tagNums = tag
    major, minor, patch = tagNums.split(".")
    # print(tag, major, minor, patch)

    if int(minor) > 1:
        tag_to_del = tag
        print(tag_to_del)

        proc = run(
            f'git push --delete origin "{tag_to_del}"',
            shell=True,
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
        )
        print(proc.stdout.decode().strip(), "\n", proc.stderr.decode().strip())

        proc = run(
            f'git tag --delete "{tag_to_del}"',
            shell=True,
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
        )
        print(proc.stdout.decode().strip(), "\n", proc.stderr.decode().strip())
