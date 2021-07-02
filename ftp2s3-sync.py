import math
import tempfile
import time
from contextlib import contextmanager
from ftplib import FTP
from pathlib import Path

from s3fs import S3FileSystem


def sync_dir(ftp_conn: FTP, fs: S3FileSystem, ftp_dir: Path, ftp_root: str, fs_root: str, dry_run: bool):
    print(f"Syncing {ftp_dir}")
    if str(ftp_dir) == ftp_root:
        fs_dir = fs_root
    else:
        fs_dir = f"{fs_root}/{ftp_dir.relative_to(ftp_root)}/"

    ftp_conn.cwd(str(ftp_dir))
    ftp_ls = [(filename, int(props["size"]), props["type"] == "dir") for filename, props in ftp_conn.mlsd()]
    fs_ls = {str(Path(entry["Key"]).relative_to(fs_dir)): int(entry["Size"]) for entry in fs.listdir(fs_dir)}

    for filename, size, is_dir in sorted(ftp_ls):
        file = ftp_dir / filename
        if is_dir:
            sync_dir(ftp_conn, fs, file, ftp_root, fs_root, dry_run)
        elif filename not in fs_ls or size != fs_ls[filename]:
            filepath = file.relative_to(ftp_root)
            key_id = f"{fs_root}/{filepath}"
            print(f"Syncing {file} to s3://{key_id} ({math.ceil(size / 1024 / 1024)} MB)")

            if not dry_run:
                with tempfile.NamedTemporaryFile(mode="rb+") as tmp:
                    i = [1]
                    total = [0]

                    def write_chunk(chunk):
                        chunk_size = len(chunk)
                        total[0] += chunk_size
                        tmp.write(chunk)
                        i[0] += 1

                    with log_time(f"Downloading from FTP {file}"):
                        ftp_conn.retrbinary(f"RETR {str(file)}", write_chunk, blocksize=12428800)
                        tmp.flush()

                    with log_time(f"Uploading to S3 {key_id}"):
                        fs.put_file(tmp.name, key_id)


def sync(
    ftp_host: str,
    ftp_username: str,
    ftp_password: str,
    ftp_dir: str,
    bucket: str,
    dry_run: bool,
):
    fs = S3FileSystem(anon=False)
    ftp_conn = FTP(ftp_host, ftp_username, ftp_password)

    try:
        sync_dir(ftp_conn, fs, Path(ftp_dir), ftp_dir, bucket, dry_run)
    finally:
        ftp_conn.close()


@contextmanager
def log_time(msg: str):
    start = time.time()
    try:
        print(f"{msg}...", end="", flush=True)
        yield
    finally:
        duration = time.time() - start
        print(f"{(duration * 1000000):0.02f} s")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sync FTP to S3", add_help=False)
    parser.add_argument("-h", "--ftp-host")
    parser.add_argument("-u", "--ftp-user")
    parser.add_argument("-p", "--ftp-password")
    parser.add_argument("-d", "--ftp-dir")
    parser.add_argument("-b", "--bucket")
    parser.add_argument("-n", "--dryrun", required=False, type=bool, default=False)
    args = parser.parse_args()

    sync(
        ftp_host=args.ftp_host,
        ftp_username=args.ftp_user,
        ftp_password=args.ftp_password,
        ftp_dir=args.ftp_dir,
        bucket=args.bucket,
        dry_run=args.dryrun,
    )
    print("Sync complete")
