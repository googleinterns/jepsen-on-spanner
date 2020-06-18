#!/usr/bin/python3
import subprocess
import os
import time
import argparse

parser = argparse.ArgumentParser('Driver for Jepsen-on-Spanner testing framework.')
parser.add_argument('--clean', type=bool, nargs='?', const=False)
parser.add_argument('--workers', type=int)
args = parser.parse_args()
clean_up = args.clean
worker_num = args.workers

os.system("./gradlew shadowJar")
os.system("gcloud builds submit --tag gcr.io/jepsen-on-spanner-with-gke/jepsen-on-spanner .")

os.system("java -jar ./build/libs/Jepsen-on-spanner-1.0-SNAPSHOT-all.jar --project "
          "jepsen-on-spanner-with-gke --instance jepsen --database test --component INIT --pID 0 "
          "--initial-values init.csv")

os.system("mkdir ./jobs")
for i in range(1, worker_num + 1):
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

os.system("java -jar ./build/libs/Jepsen-on-spanner-1.0-SNAPSHOT-all.jar --project "
          "jepsen-on-spanner-with-gke --instance jepsen --database test --component VERIFIER "
          "--pID 0 --initial-values init.csv")

os.system("rm -r ./jobs")

if clean_up:
    for i in range(1, worker_num + 1):
        os.system(f"kubectl delete job test-worker-{i}")
os.system("gcloud spanner databases delete test --instance=jepsen")