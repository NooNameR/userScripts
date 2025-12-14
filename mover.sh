#!/usr/bin/env bash
set -euo pipefail

HC_BASE_URL="${1:-}"
HC_UUID="${2:-}"

if [[ -n "${HC_BASE_URL}" && -n "${HC_UUID}" ]]; then
  HC_URL="${HC_BASE_URL%/}/ping/${HC_UUID}"
  HC_LOG="${HC_URL}/log"
  HC_ENABLED=true
else
  HC_ENABLED=false
fi

# ---- healthchecks failure + start ----
if [[ "${HC_ENABLED}" == true ]]; then
  trap 'ec=$?; curl -fsS "${HC_URL}/fail?exit=${ec}" || true; exit $ec' ERR
  curl -fsS "${HC_URL}/start" || true
fi

# ---- healthchecks log helper (log only) ----
hc_log() {
  [[ "${HC_ENABLED}" == true ]] || return 0
  printf '%s\n' "$1" | curl -fsS --data-binary @- "${HC_LOG}" || true
}

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
[[ -f "${REQS}" ]] || { echo "ERROR: requirements.txt not found"; exit 1; }
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
hc_log "cache mover started"

"${VENV_DIR}/bin/python" "${SCRIPT}" \
  --config "${CONFIGFILE}" \
  --log-file "/var/log/cache_mover.log" \
  --lock-file "${LOCKFILE}"

hc_log "cache mover finished"

# ---- fclones ----
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

hc_log "fclones started"

for pair in "${folders[@]}"; do
  IFS="|" read -r src dst <<< "${pair}"

  log "fclones started: ${src} <-> ${dst}"

  /usr/bin/fclones group \
    --one-fs \
    --hidden \
    --follow-links \
    "${DATA_DIR}/${src}" \
    "${DATA_DIR}/${dst}" \
  | /usr/bin/fclones link

  log "fclones finished: ${src} <-> ${dst}"
done

hc_log "fclones finished"

# ---- SnapRAID ----
log "Starting SnapRAID"
hc_log "snapraid started"

/usr/bin/snapraid-daily

log "SnapRAID completed"
hc_log "snapraid completed"

# ---- healthchecks success ----
if [[ "${HC_ENABLED}" == true ]]; then
  curl -fsS "${HC_URL}" || true
fi