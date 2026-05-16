
## Installing on target hosts

### Scenario 1: Monolith (single VM)

```bash
VERSION="0.1.0"
wget https://github.com/Lordnns/MMORPG/releases/download/v${VERSION}/mmorpg-monolith_${VERSION}_all.deb
sudo apt install ./mmorpg-monolith_${VERSION}_all.deb
sudo nano /etc/default/mmorpg-monolith    # review settings
sudo systemctl start mmorpg-monolith
```

### Scenario 2: Central + Fleet

**On the control plane VM:**
```bash
VERSION="0.1.0"
wget https://github.com/Lordnns/MMORPG/releases/download/v${VERSION}/mmorpg-central-fleet_${VERSION}_all.deb
sudo apt install ./mmorpg-central-fleet_${VERSION}_all.deb
sudo nano /etc/default/mmorpg-central-fleet    # set DS_HOSTS, AGENT_TOKEN, ORCH_ADVERTISE_HOST
sudo systemctl start mmorpg-central-fleet
```

**On each DS host (1..N):**
```bash
VERSION="0.1.0"
wget https://github.com/Lordnns/MMORPG/releases/download/v${VERSION}/mmorpg-fleet-ds_${VERSION}_amd64.deb
sudo apt install ./mmorpg-fleet-ds_${VERSION}_amd64.deb
sudo nano /etc/default/mmorpg-host-agent    # set REDIS_URL, AGENT_TOKEN
sudo systemctl start mmorpg-host-agent
```

### Scenario 3: Fully distributed (one component per VM)

**Redis VM:**
```bash
VERSION="0.1.0"
wget https://github.com/Lordnns/MMORPG/releases/download/v${VERSION}/mmorpg-redis_${VERSION}_amd64.deb
sudo apt install ./mmorpg-redis_${VERSION}_amd64.deb
sudo systemctl start mmorpg-redis
```
**Gatekeeper VM:**
```bash
VERSION="0.1.0"
wget https://github.com/Lordnns/MMORPG/releases/download/v${VERSION}/mmorpg-gatekeeper_${VERSION}_amd64.deb
sudo apt install ./mmorpg-gatekeeper_${VERSION}_amd64.deb
sudo nano /etc/default/mmorpg-gatekeeper    # edit REDIS_URL
sudo systemctl start mmorpg-gatekeeper
```
**Orchestrator VM:**
```bash
VERSION="0.1.0"
wget https://github.com/Lordnns/MMORPG/releases/download/v${VERSION}/mmorpg-orchestrator_${VERSION}_amd64.deb
sudo apt install ./mmorpg-orchestrator_${VERSION}_amd64.deb
sudo nano /etc/default/mmorpg-orchestrator    # edit REDIS_URL, DS_HOSTS, AGENT_TOKEN, ORCH_ADVERTISE_HOST
sudo systemctl start mmorpg-orchestrator
```
**DS VMs (N):**
```bash
VERSION="0.1.0"
wget https://github.com/Lordnns/MMORPG/releases/download/v${VERSION}/mmorpg-fleet-ds_${VERSION}_amd64.deb
sudo apt install ./mmorpg-fleet-ds_${VERSION}_amd64.deb
sudo nano /etc/default/mmorpg-host-agent    # edit REDIS_URL, AGENT_TOKEN
sudo systemctl start mmorpg-host-agent
```