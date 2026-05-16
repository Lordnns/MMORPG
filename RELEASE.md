# Cutting a release

The pipeline builds Docker images + .deb packages every time you push
a git tag matching `v*`.

## Prerequisites (one-time GitHub setup)

1. **GHCR permissions** — the workflow uses `GITHUB_TOKEN`, which already has
   `packages: write` per the `permissions:` block in `release.yml`. Nothing
   to configure.

2. **First image push will be private by default.** After the first release,
   go to https://github.com/Lordnns?tab=packages, click each package
   (mmorpg-orchestrator, mmorpg-gatekeeper, mmorpg-dedicated-server), then
   Package settings → Change visibility → Public. Do this once per package.

3. **Update IMAGE_NAMESPACE in release.yml** if your GitHub username differs
   from `lordnns`. Look at the `env:` block at the top of the workflow.

## Cutting a release

```bash
# From your dev box, on main with the changes you want to ship:
git tag v0.1.0
git push origin v0.1.0
```

The pipeline runs automatically. Watch progress at:
https://github.com/Lordnns/MMORPG/actions

Once complete (≈ 10–15 min), check:
- https://github.com/Lordnns/MMORPG/releases — `.deb` files attached
- https://github.com/Lordnns?tab=packages — Docker images pushed

## Installing on target hosts

### Scenario 1: Monolith (single VM)

```bash
wget https://github.com/Lordnns/MMORPG/releases/download/v0.1.0/mmorpg-monolith_0.1.0_all.deb
sudo apt install ./mmorpg-monolith_0.1.0_all.deb
sudo vim /etc/default/mmorpg-monolith    # review settings
sudo systemctl start mmorpg-monolith
```

### Scenario 2: Central + Fleet

On the control plane VM:
```bash
wget .../mmorpg-central-fleet_0.1.0_all.deb
sudo apt install ./mmorpg-central-fleet_0.1.0_all.deb
sudo vim /etc/default/mmorpg-central-fleet    # set DS_HOSTS, AGENT_TOKEN, ORCH_ADVERTISE_HOST
sudo systemctl start mmorpg-central-fleet
```

On each DS host (1..N):
```bash
wget .../mmorpg-fleet-ds_0.1.0_amd64.deb
sudo apt install ./mmorpg-fleet-ds_0.1.0_amd64.deb
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
git push origin :refs/tags/v0.1.0   # delete remote tag
git tag -d v0.1.0                   # delete local tag
# Delete the corresponding GitHub release through the web UI first.
# Then re-tag and push:
git tag v0.1.0
git push origin v0.1.0
```

## Testing the .deb build locally before tagging

```bash
# From repo root.
docker run --rm -v "$PWD:/work" -w /work debian:13 bash -c '
    apt update && apt install -y dpkg-dev fakeroot
    HOST_AGENT_BIN=/work/VMCode/Host_Agent/target/release/host_agent \
    VERSION=0.0.0-dev \
    IMAGE_NAMESPACE=lordnns \
    ./debian/build-debs.sh
'
ls -lh dist/
```

(Requires you to have already built `host_agent` locally with `cargo build --release` in `VMCode/Host_Agent`.)
