#!/usr/bin/python3
import subprocess
import os

os.system("./gradlew shadowJar")
os.system("java -jar ./build/libs/Jepsen-on-spanner-1.0-SNAPSHOT-all.jar --instance jepsen "
                "--database test --component INIT --pID 0 --initial-values init.csv")
os.system("mkdir ./jobs")
for i in range(1, 11):
    os.system(f"cat deployment.yaml | sed \"s/\\$PID/{i}/\" > ./jobs/job-{i}.yaml")
os.system("kubectl create -f ./jobs")
os.system("java -jar ./build/libs/Jepsen-on-spanner-1.0-SNAPSHOT-all.jar --instance jepsen "
                "--database test --component VERIFIER --pID 0 --initial-values init.csv")
