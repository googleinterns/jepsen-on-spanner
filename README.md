# Jepsen on Spanner

**This is not an officially supported Google product.**

Jepsen is a correctness verifcation tools for distributed systems for their
consistency under special scenarios. This project intended to apply Jepsen on
Cloud Spanner databases.

##Installation
Make sure you have Java 11, Python 3 and `kubectl` installed.

Download this repo.

`git clone https://github.com/googleinterns/jepsen-on-spanner.git`

You need to set up a kubernetes cluster and port the settings to `kuberctl` as well. Using Google
Kubernetes Engine, execute:
 
```
gcloud container clusters create [cluster-name]
gcloud container clusters get-credentials [cluster-name]
```

##Usage

`python main.py [OPTION]`

####Options

`--project` specify the ID of the GCP project

`--instance` specify the Cloud Spanner instance to run test on

`--database` specify the name of the database to be created and tested on

`--delete, -d` cleanup kubernetes jobs / Spanner instance

`--redeploy, -r` redeploy the Jepsen-on-spanner executable to remote image

`--worker, -w` specify number of workers

`--benchmark, -b` specify the type of benchmark to run; supports `linearizability` and `bank`

`--job, -j` if specified, will run until invalid history found or error occurs

####Example

To run a linearizability benchmark once on 8 workers:

`python main.py -w 8 -b linearizability --project [projectID] --instance [instanceID] --database
[databaseID]`

To run a bank benchmark on 5 workers, redeploy the image on kubernetes, until failure:

`python main.py -w 5 -r -j -b bank --project [projectID] --instance [instanceID] --database
                                  [databaseID]`

To cleanup a previous job with 3 workers:

`python main.py -w 3 -d --project [projectID] --instance [instanceID] --database [databaseID]`

## Workflow

![workflow](workflow.png "workflow")

The testing framework is consisted of three components: a Generator, an Executor and a Verifier
. The verifier component will be ran on the local machine, while the generator and executor gets
 packaged and deployed to remote workers (like kubernetes) to achieve concurrency and scalability.
 - Generator generates random Spanner operations
 - Executor maintains the connection with the Spanner instance, persists logs interaction
  history with the Spanner instance, as a result of client calls issued from the load generator and reads the records out to a log history file in EDN format
 - Verifier validates the log history, written by the executor, to make sure the history
  reflects a certain consistency assumption

## File Structure
- package `com.google.jepsenonspanner.loadgenerator` contains the generator component
- package `com.google.jepsenonspanner.client` contains the executor component
- package `com.google.jepsenonspanner.verifier` contains the verifier component
- `JepsenOnSpanner.java` contains the component driver
- `main.py` contains the driver for the whole testing framework
- `generate_keys.py` contains a helper program to generate multiple keys of large size; can be
 used for stress testing
 
 ## Credit
 Inspired by [Jepsen](http://jepsen.io/) and [Knossos](https://github.com/googleinterns/jepsen-on-spanner).
