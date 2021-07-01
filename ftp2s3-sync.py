import math
import tempfile
from ftplib import FTP
from pathlib import Path

from s3fs import S3FileSystem


def move_file(ftp_conn: FTP, fs: S3FileSystem, file: Path, size: int, ftp_root: str, fs_root: str, dry_run: bool):
    filepath = file.relative_to(ftp_root)

    key_id = f"{fs_root}/{filepath}"

    if fs.exists(key_id):
        # check if we need to replace, check sizes
        if size == fs.size(key_id):
            print("%s already uploaded" % key_id)
            return

    if not dry_run:
        with tempfile.NamedTemporaryFile(mode="rb+") as tmp:
            i = [1]
            total = [0]

            def write_chunk(chunk):
                chunk_size = len(chunk)
                total[0] += chunk_size
                tmp.write(chunk)
                i[0] += 1

            print(f"{key_id} downloading from FTP ({math.ceil(size / 1024 / 1024)} MB)")
            ftp_conn.retrbinary(f"RETR {str(file)}", write_chunk, blocksize=12428800)
            tmp.flush()
            print(f"{key_id} uploading to S3 ({math.ceil(size / 1024 / 1024)} MB)")
            fs.put_file(tmp.name, key_id)


def sync_dir(ftp_conn: FTP, fs: S3FileSystem, directory: Path, ftp_root: str, key_root: str, dry_run: bool):
    ftp_conn.cwd(str(directory))
    for filename, props in ftp_conn.mlsd():
        file = directory / filename
        if props["type"] == "dir":
            sync_dir(ftp_conn, fs, file, ftp_root, key_root, dry_run)
        else:
            move_file(ftp_conn, fs, file, int(props["size"]), ftp_root, key_root, dry_run)


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
