#!/usr/bin/python3
import subprocess
import os
import time
import argparse

parser = argparse.ArgumentParser(
    'Driver for Jepsen-on-Spanner testing framework.')
parser.add_argument('--redeploy', '-r', action='store_true', help='if specified, will redeploy '
                                                                  'the program to Google Cloud '
                                                                  'build as a docker image')
parser.add_argument('--workers', '-w', type=int, required=True, help='number of concurrently '
                                                                     'running workers')
parser.add_argument('--benchmark', '-b', type=str, help='type of benchmark to run')
parser.add_argument('--job', '-j', action='store_true', help='if specified, will keep running '
                                                             'till fail')
parser.add_argument('--delete', '-d', action='store_true', help='if specified, will clean up '
                                                                'workers and spanner database')
args = parser.parse_args()
worker_num = args.workers
redeploy = args.redeploy
is_job = args.job
delete = args.delete
benchmark = args.benchmark
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
            f"cat deployment.yaml | sed \"s/\\$PID/{i}/\" | sed \"s/\\$BENCHMARK/{benchmark}/\" > "
            f"./jobs/job-{i}.yaml")
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
         "VERIFIER", "--pID", "0", "--initial-values", "init.csv", "--benchmark-type", benchmark],
        stdout=subprocess.PIPE).stdout.decode("utf-8")
    print(output)

    if "Invalid operation found" in output:
        global fail
        fail = True
    else:
        clean_up()


def clean_up():
    global worker_num
    for i in range(1, worker_num + 1):
        os.system(f"kubectl delete job test-worker-{i}")
    process = subprocess.Popen(["gcloud", "spanner", "databases", "delete", "test",
                                "--instance=jepsen"], stdin=subprocess.PIPE)
    process.communicate(input=b'Y')
    os.system("rm -r ./jobs")


if delete:
    clean_up()
else:
    deploy()
    if is_job:
        while not fail:
            run()
            time.sleep(5)
    else:
        run()
