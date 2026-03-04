#!usr/bin/env python3

import os
import sys

msg_comm=sys.argv[1]

with open(msg_comm,"r",encoding="utf-8") as f:
    msg=f.read().strip()


if not (msg.startswith("feat: ") or msg.startswith("fix: ") or msg.startswith("chore: ")):
    print("Use Conventional Commit Message that starts with (feat: ,fix: ,chore: )")
    exit(1)
    
print("Conventional Message pattern matched!")
exit(0)    