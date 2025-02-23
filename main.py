import lz4.frame
from io import BytesIO
import datetime
import argparse
from pathlib import Path
from glob import glob
from dataclasses import dataclass
import json

@dataclass
class Block:
    offset: int
    checksum: str
     
    def resolve_block_path(self, backup_path: Path) -> Path | None: 
        try:
            return Path(glob(f"{backup_path}/blocks/**/**/{self.checksum}.blk")[0])
        except IndexError:
            return None
    

@dataclass
class Backup:
    identifier: str
    timestamp: datetime.datetime
    size: int
    compression: str
    blocks: list[Block]


@dataclass
class VolumeBackup:
    name: str
    backup_path: Path
    backups: list[Backup]


def decompress_lz4(block_path: Path):
    return lz4.frame.decompress(block_path.read_bytes())

def write_block_to_buffer(offset: int, block_data: bytes, buffer: BytesIO) -> None:
    buffer.seek(offset)
    buffer.write(block_data)
    
def find_backup_path(backup_root, backup_target) -> Path:
    try: 
        return [Path(v) for v in glob(f"{backup_root}/volumes/**/**/{backup_target}")][0]
    except IndexError:
        raise ValueError(f"Could not find backup for {backup_target}")

def read_backups(path: Path) -> VolumeBackup: 
    backup_cfg_paths = glob(str(path / "backups" / "*.cfg"))
    volume_backup = VolumeBackup(
        name=path.name,
        backup_path=path,
        backups=[]
    )
    for backup_cfg_path in backup_cfg_paths:
        with open(backup_cfg_path, "rb") as f:
            backup_cfg = json.load(f)
            volume_backup.backups.append(Backup(
                identifier=backup_cfg_path,
                timestamp=datetime.datetime.fromisoformat(backup_cfg["CreatedTime"]),
                size=int(backup_cfg["Size"]),
                compression=backup_cfg["CompressionMethod"],
                blocks=[Block(offset=int(block["Offset"]), checksum=block["BlockChecksum"]) for block in backup_cfg["Blocks"]]
            ))
    volume_backup.backups.sort(key=lambda b: b.timestamp)
    return volume_backup

    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backup-root", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--outfile", required=True)

    args = parser.parse_args()

    if not (Path(args.backup_root) / "backupstore").exists():
        raise ValueError(f"Backup root {args.backup_root} does not contain backupstore")

    if not (Path(args.outfile).parent).exists():
        raise ValueError(f"Output file {args.outfile} does not exist")

    if Path(args.outfile).exists():
        raise ValueError(f"Output file {args.outfile} already exists")

    backup_root = Path(args.backup_root) / "backupstore"
    path = find_backup_path(backup_root, args.target)
    volume_backup = read_backups(path)
    buffer = BytesIO(b"\0" * volume_backup.backups[-1].size)

    for backup in volume_backup.backups:
        for block in backup.blocks:
            block_path = block.resolve_block_path(volume_backup.backup_path)
            if block_path is None:
                raise ValueError(f"Could not find block for {block}")
            block_data = decompress_lz4(block_path)
            write_block_to_buffer(block.offset, block_data, buffer)

    with open(args.outfile, "wb") as f:
        f.write(buffer.getvalue())

if __name__ == "__main__":
    main()
