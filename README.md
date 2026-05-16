
## Installing on target hosts

### Scenario 1: Monolith (single VM)

```bash
wget [https://github.com/](https://github.com/)<USERNAME>/<REPO>/releases/download/v<VERSION>/mmorpg-monolith_<VERSION>_all.deb
sudo apt install ./mmorpg-monolith_<VERSION>_all.deb
sudo vim /etc/default/mmorpg-monolith    # review settings
sudo systemctl start mmorpg-monolith
```

### Scenario 2: Central + Fleet

On the control plane VM:
```bash
wget [https://github.com/](https://github.com/)<USERNAME>/<REPO>/releases/download/v<VERSION>/mmorpg-central-fleet_<VERSION>_all.deb
sudo apt install ./mmorpg-central-fleet_<VERSION>_all.deb
sudo vim /etc/default/mmorpg-central-fleet    # set DS_HOSTS, AGENT_TOKEN, ORCH_ADVERTISE_HOST
sudo systemctl start mmorpg-central-fleet
```

On each DS host (1..N):
```bash
wget [https://github.com/](https://github.com/)<USERNAME>/<REPO>/releases/download/v<VERSION>/mmorpg-fleet-ds_<VERSION>_amd64.deb
sudo apt install ./mmorpg-fleet-ds_<VERSION>_amd64.deb
sudo vim /etc/default/mmorpg-host-agent    # set REDIS_URL, AGENT_TOKEN
sudo systemctl start mmorpg-host-agent
```

### Scenario 3: Fully distributed (one component per VM)

- Redis VM:        `sudo apt install ./mmorpg-redis_*.deb`
- Gatekeeper VM:   `sudo apt install ./mmorpg-gatekeeper_*.deb` (edit REDIS_URL)
- Orchestrator VM: `sudo apt install ./mmorpg-orchestrator_*.deb` (edit REDIS_URL, DS_HOSTS, AGENT_TOKEN, ORCH_ADVERTISE_HOST)
- DS VMs (N):      `sudo apt install ./mmorpg-fleet-ds_*.deb` (edit REDIS_URL, AGENT_TOKEN)

## Re-running a release

If a release fails partway and you need to retry the same tag:

```bash
git push origin :refs/tags/v<VERSION>   # delete remote tag
git tag -d v<VERSION>                   # delete local tag
# Delete the corresponding GitHub release through the web UI first.
# Then re-tag and push:
git tag v<VERSION>
git push origin v<VERSION>
```

## Testing the .deb build locally before tagging

```bash
# From repo root.
docker run --rm -v "$PWD:/work" -w /work debian:13 bash -c '
    apt update && apt install -y dpkg-dev fakeroot
    HOST_AGENT_BIN=/work/VMCode/Host_Agent/target/release/host_agent \
    VERSION=0.0.0-dev \
    IMAGE_NAMESPACE=<USERNAME> \
    ./debian/build-debs.sh
'
ls -lh dist/
```

(Requires you to have already built `host_agent` locally with `cargo build --release` in `VMCode/Host_Agent`.)