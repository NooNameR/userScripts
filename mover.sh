#!/usr/bin/env bash
set -euo pipefail

# ---- paths ----
BASE_DIR="/opt/mover"
VENV_DIR="${BASE_DIR}/mover/venv"
SCRIPT="${BASE_DIR}/mover/cache_mover.py"
REQS="${BASE_DIR}/mover/requirements.txt"

CONFIGFILE="/etc/mover/config.yml"
LOGFILE="/var/log/mover.log"
LOCKFILE="/var/run/mover.pid"

DATA_DIR="/mnt/storage/data"

# ---- logging helper ----
log() {
  echo "[$(date -Is)] $*" >> "${LOGFILE}"
}

# ---- sanity checks ----
[[ -f "${SCRIPT}" ]] || { echo "ERROR: cache_mover.py not found"; exit 1; }
[[ -f "${REQS}" ]]   || { echo "ERROR: requirements.txt not found"; exit 1; }
[[ -f "${CONFIGFILE}" ]] || { echo "ERROR: config.yml not found"; exit 1; }

# ---- ensure runtime paths ----
mkdir -p /var/run

# ---- ensure venv exists ----
if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  log "Creating virtualenv"
  python3 -m venv "${VENV_DIR}"
  log "Virtualenv ready"
fi

"${VENV_DIR}/bin/pip" install --upgrade pip
"${VENV_DIR}/bin/pip" install -r "${REQS}"

# ---- run mover ----
log "Starting cache mover"

"${VENV_DIR}/bin/python" "${SCRIPT}" \
  --config "${CONFIGFILE}" \
  --log-file "/var/log/cache_mover.log" \
  --lock-file "${LOCKFILE}"

folders=(
  "torrents/en/tv|media/tv"
  "torrents/en/tv|torrents/en/cross-seed"

  "torrents/en/movies-uhd|media/movies-uhd"
  "torrents/en/movies-uhd|media/movies-uhd-kids"
  "torrents/en/movies-uhd|torrents/en/cross-seed"

  "torrents/en/movies-hd|media/movies-hd"
  "torrents/en/movies-hd|media/movies-hd-kids"
  "torrents/en/movies-hd|torrents/en/cross-seed"

  "torrents/ua/movies|media/movies-ua"
  "torrents/ua/movies|media/movies-ua-kids"
  "torrents/ua/movies|torrents/ua/cross-seed"

  "torrents/ua/tv|media/tv-ua"
  "torrents/ua/tv|torrents/ua/cross-seed"
)

for pair in "${folders[@]}"; do
    IFS="|" read -r src dst <<< "${pair}"

    log "fclones started: ${src} <-> ${dst}"

    result=$(/usr/bin/fclones group --one-fs --hidden --follow-links $DATA_DIR/$src $DATA_DIR/$dst | /usr/bin/fclones link)

    log "fclones finished: ${src} <-> ${dst}"
done

# ---- SnapRAID ----
log "Starting SnapRAID"
/usr/bin/snapraid-daily
log "SnapRAID completed"
