"""Explicit offline migration; stop the IoT agent first. Never creates a source DB."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from plcnext_iot.storage.outbox_migration import migrate_existing


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',required=True,type=Path)
    parser.add_argument('--gateway',required=True)
    parser.add_argument('--backup',required=True,type=Path)
    args=parser.parse_args()
    print(json.dumps(migrate_existing(args.database,args.gateway,args.backup),ensure_ascii=False,indent=2))


if __name__=='__main__':main()
