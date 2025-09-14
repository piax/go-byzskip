# dhtsrv

It runs as a ByzSkip DHT node. Note that this is an experimental implementation.

## Usage

```bash
# Run as bootstrap node
./dhtsrv -b -apub <authority-public-key> -repo-dir /path/to/repo

# Run as regular node
./dhtsrv -apub <authority-public-key> -i <bootstrap-addr> -repo-dir /path/to/repo
```

## Options

- `-b`: Run as bootstrap node
- `-apub`: Authority public key (required)
- `-repo-dir`: Repository directory containing pcert file (optional)
  - If specified, automatically uses `repo-dir/pcert` as the certificate file
  - If not specified, uses `BS_CERT_FILE` environment variable
- `-i`: Bootstrap node address (ignored when `-b` is used)
- `-k`: Redundancy factor (default: 4)
- `-p`: P2P port (default: 9000)
- `-s`: Web API port (default: 8000)
- `-v`: Verbose output
- `-t`: Use TCP instead of QUIC
- `-insecure`: Run without authority (not recommended)
- `-auth`: Authority web URL (default: http://localhost:7001)

## Certificate File Detection

The program automatically detects the certificate file in the following order:

1. If `-repo-dir` is specified, looks for `repo-dir/pcert`
2. If `BS_CERT_FILE` environment variable is set, uses that file
3. If neither is available, returns an error

## Web API

The web API server runs on the specified port (default: 8000) and provides the following endpoints:

- `GET /get/*name`: Get a value by name
- `PUT /put/*name`: Put a value by name
- `GET /tab`: Show routing table
- `GET /stat`: Show node statistics

## Example

```bash
# Set up repository with pcert file
mkdir -p /tmp/my-repo
cp /path/to/certificate.json /tmp/my-repo/pcert

# Run DHT node using repo directory
./dhtsrv -apub BABBEIIDM7V3FR4RWNGVXYRSHOCL6SYWLNIJLP4ONDGNB25HS7PKE6C56M2Q -repo-dir /tmp/my-repo
```