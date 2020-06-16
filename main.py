#!/usr/bin/python3
import subprocess
import os
import time

os.system("./gradlew shadowJar")
os.system("java -jar ./build/libs/Jepsen-on-spanner-1.0-SNAPSHOT-all.jar --instance jepsen "
                "--database test --component INIT --pID 0 --initial-values init.csv")

os.system("mkdir ./jobs")
for i in range(1, 11):
    os.system(f"cat deployment.yaml | sed \"s/\\$PID/{i}/\" > ./jobs/job-{i}.yaml")
os.system("kubectl create -f ./jobs")

in_progress = True
while in_progress:
    in_progress = False
    output = subprocess.run(['kubectl', 'get', 'pods'], stdout=subprocess.PIPE).stdout.decode(
        'utf-8')
    lines = output.split("\n")
    for line in lines[1:]:
        fields = line.split()
        if len(fields) == 0:
            continue
        print(fields)
        if fields[2] != "Completed":
            in_progress = True
            time.sleep(3)
            break

os.system("java -jar ./build/libs/Jepsen-on-spanner-1.0-SNAPSHOT-all.jar --instance jepsen "
                "--database test --component VERIFIER --pID 0 --initial-values init.csv")
