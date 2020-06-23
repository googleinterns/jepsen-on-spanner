#!/usr/bin/python3
import subprocess
import os
import time
import argparse

parser = argparse.ArgumentParser(
    'Driver for Jepsen-on-Spanner testing framework.')
parser.add_argument('--redeploy', '-r', action='store_true')
parser.add_argument('--workers', '-w', type=int, required=True)
parser.add_argument('--job', '-j', action='store_true')
args = parser.parse_args()
worker_num = args.workers
redeploy = args.redeploy
is_job = args.job
fail = False


def deploy():
    os.system("./gradlew shadowJar")

    if redeploy:
        # redeploy to gcloud to build docker image
        os.system(
            "gcloud builds submit --tag gcr.io/jepsen-on-spanner-with-gke/jepsen-on-spanner ."
        )


def run():
    # Run the set up:
    # 1. create the testing and history tables
    # 2. insert the initial key value pairs in init.csv
    os.system(
        "java -jar ./build/libs/Jepsen-on-spanner-1.0-SNAPSHOT-all.jar --project "
        "jepsen-on-spanner-with-gke --instance jepsen --database test --component INIT --pID 0 "
        "--initial-values init.csv")

    # Generate YAML deployment files from template and deploy to kubernetes
    os.system("mkdir ./jobs")
    for i in range(1, worker_num + 1):
        os.system(
            f"cat deployment.yaml | sed \"s/\\$PID/{i}/\" > ./jobs/job-{i}.yaml")
    os.system("kubectl create -f ./jobs")

    # Poll for status of the pods and start verifier only when all workers finish
    in_progress = True
    while in_progress:
        in_progress = False
        output = subprocess.run(['kubectl', 'get', 'pods'],
                                stdout=subprocess.PIPE).stdout.decode('utf-8')
        lines = output.split("\n")
        for line in lines[1:]:
            fields = line.split()
            if len(fields) == 0:
                continue
            print(fields)
            if fields[2] != "Completed":
                in_progress = True
                time.sleep(2)
                break

    output = subprocess.run(
        ["java", "-jar", "./build/libs/Jepsen-on-spanner-1.0-SNAPSHOT-all.jar", "--project",
         "jepsen-on-spanner-with-gke", "--instance", "jepsen", "--database", "test", "--component",
         "VERIFIER", "--pID", "0", "--initial-values", "init.csv"],
        stdout=subprocess.PIPE).stdout.decode("utf-8")
    print(output)

    if "Valid!" in output:
        for i in range(1, worker_num + 1):
            os.system(f"kubectl delete job test-worker-{i}")
        process = subprocess.Popen(["gcloud", "spanner", "databases", "delete", "test",
                                    "--instance=jepsen"], stdin=subprocess.PIPE)
        process.communicate(input=b'Y')
    else:
        global fail
        fail = True

    os.system("rm -r ./jobs")



deploy()
if is_job:
    while not fail:
        run()
        time.sleep(5)
else:
    run()
