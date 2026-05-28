# distributed-stream-processing4

## Python Environment Setup

### Create Virtual Environment

* `python -m venv venv` → Create isolated Python environment
  Creates a local virtual environment named `venv` so project dependencies remain isolated from global system packages. Prevents version conflicts between projects.

### Activate Virtual Environment

#### Windows

* `venv\Scripts\activate`

#### Linux / macOS

* `source venv/bin/activate`

Activating the environment changes your shell context so all Python packages install locally inside the project.

---

### Upgrade pip

* `python -m pip install --upgrade pip` → Upgrade package manager
  Updates pip to the latest version for better dependency resolution and compatibility.

---

### Install Project Dependencies

* `pip install kafka-python` → Install Kafka client library
  Installs the Python Kafka client used for building producers and consumers programmatically.

* `pip install confluent-kafka` → High-performance Kafka client
  Installs Confluent’s optimized Kafka client built on top of `librdkafka`. Faster and more production-grade than pure Python implementations.

* `pip install -r requirements.txt` → Install all dependencies
  Reads dependencies from `requirements.txt` and installs them automatically.

---

### Generate requirements.txt

* `pip freeze > requirements.txt` → Save installed dependencies
  Captures all installed package versions so the environment can be recreated consistently on another machine.

---

### Verify Installed Packages

* `pip list` → Show installed Python packages
  Displays all installed packages and versions inside the active virtual environment.

---

### Deactivate Virtual Environment

* `deactivate` → Exit virtual environment
  Returns the shell back to the system Python environment.

---

## Docker Commands

* `docker compose up -d` → Start Kafka containers in background
  Starts all services defined in docker-compose.yml as detached background processes. You get your terminal back immediately.

* `docker ps` → Show running containers
  Lists all currently running containers with their IDs, names, ports, and uptime. Use this to confirm Kafka and Zookeeper are actually up.

* `docker exec -it kafka bash` → Enter Kafka container shell
  Opens an interactive bash shell inside the running Kafka container. From here you can run all kafka-* CLI commands directly.

* `docker compose down` → Stop and remove containers
  Stops all running containers and removes them along with their networks. Data volumes are preserved unless you add `-v` flag.

* `docker compose restart` → Restart Kafka stack
  Stops and restarts all services in docker-compose.yml. Useful when Kafka gets into a bad state without full teardown.

---

## Topic Commands

* `kafka-topics --create --topic movies --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1` → Create topic
  Creates a new topic named "movies" with 3 partitions for parallelism and replication factor 1 (suitable for local/single-broker setup). Partitions determine how many consumers can read in parallel.

* `kafka-topics --list --bootstrap-server localhost:9092` → List all topics
  Prints all existing topic names on the broker. Use this to verify your topic was created or to see what topics are available.

* `kafka-topics --describe --topic movies --bootstrap-server localhost:9092` → Show topic details
  Shows detailed info about the topic including partition count, replication factor, leader broker, and ISR (in-sync replicas) for each partition.

* `kafka-topics --delete --topic movies --bootstrap-server localhost:9092` → Delete topic
  Permanently deletes the topic and all its data. Cannot be undone. Make sure no active producers or consumers are using it before deleting.

---

## Producer Commands

* `kafka-console-producer --topic movies --bootstrap-server localhost:9092` → Send messages to topic
  Opens an interactive prompt where each line you type is sent as one message to the topic. Press Ctrl+C to exit. Useful for manual testing and injecting test data.

---

## Consumer Commands

* `kafka-console-consumer --topic movies --bootstrap-server localhost:9092 --from-beginning` → Read messages from start
  Reads and prints all messages in the topic from offset 0 (the very beginning). Great for verifying what data exists in the topic.

* `kafka-console-consumer --topic movies --bootstrap-server localhost:9092 --group my-group` → Read messages using consumer group
  Reads messages as part of a named consumer group. Kafka tracks the offset for this group so if you restart it picks up where it left off instead of re-reading everything.

---

## Consumer Group Commands

* `kafka-consumer-groups --list --bootstrap-server localhost:9092` → List consumer groups
  Prints all registered consumer group names on the broker. Useful to see which applications are actively consuming or have consumed from Kafka.

* `kafka-consumer-groups --describe --group my-group --bootstrap-server localhost:9092` → Show group status and lag
  Shows per-partition details for the group: current offset, log-end offset, and lag (how many messages behind the consumer is). Lag > 0 means the consumer is falling behind the producer.

---

## Debug Commands

* `kafka-broker-api-versions --bootstrap-server localhost:9092` → Check broker info
  Connects to the broker and lists all supported API versions. Use this to confirm the broker is reachable, check its version, and diagnose client-broker compatibility issues.

---

## Useful Kafka Concepts

### Partitions

Partitions are Kafka’s unit of parallelism.
If a topic has:

* 1 partition → only 1 consumer can actively read in a consumer group
* 3 partitions → up to 3 consumers can process data in parallel

More partitions increase throughput but also increase coordination overhead.

---

### Consumer Groups

Consumer groups provide scalability and fault tolerance.

Example:

* Topic has 3 partitions
* Group has 3 consumers

Each consumer gets one partition.
If one consumer crashes, Kafka rebalances and redistributes partitions automatically.

---

### Offsets

Offsets are sequential IDs assigned to messages inside each partition.

Kafka stores:

* current read position
* committed offset per consumer group

This enables:

* replaying data
* fault recovery
* exactly-once / at-least-once processing patterns

---

### Replication Factor

Replication creates backup copies of partitions across brokers.

Example:

* replication-factor=3
* one leader replica
* two follower replicas

If the leader broker crashes, a follower becomes the new leader.

Local Docker setups usually use replication-factor=1 because only one broker exists.

---

### Lag

Lag = messages produced − messages consumed

High lag indicates:

* slow consumers
* overloaded processing
* network bottlenecks
* insufficient partitions

Monitoring lag is critical in production stream-processing systems.
