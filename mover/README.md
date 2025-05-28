# Cache Mover

A simple script to move data from hot storage to cold storage **while preserving hardlinks**.  
It includes built-in qBittorrent integration that allows pausing torrents based on the files that need to be moved.

## Configuration

1. Clone or download the repository, then install dependencies:
```bash
cd mover
pip install -r requirements.txt
```
2. Create a configuration file using `config.sample.yaml` as an example.

## Execution
Run the script with your config file:
```bash
python cache_mover.py --config config.yaml
```

For UnRAID it is suggested to pass `--lock-file` to OS consider execution as native mover run:
```bash
python cache_mover.py --config config.yaml --lock-file /var/run/mover.pid
```