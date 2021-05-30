# Graphsurge Dockerfile

```bash
$ cp Dockerfile.sample Dockerfile
$ vim Dockerfile # set [uid] and [gid] to $(id -u) and $(id -g) if your user.
$ docker build -t graphsurge .
$ docker run --rm -v ~:/opt --user graphsurge:graphsurge graphsurge bash -c "cd /opt/[experiment] && ../run_computation_ext.sh [computation] [type]"
```
