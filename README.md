# queuectl

`queuectl` is a small command-line based background job queue that I built for the QueueCTL Backend Internship assignment.  
The idea is to have a system where you can push jobs into a queue, and workers will pick them up and execute them in the background – similar to how companies handle email sending, report generation, notifications etc.

I kept everything simple and understandable, but still tried to cover the important production concepts like:
- Multiple workers processing tasks
- Safe locking so two workers don’t run the same job
- Retry mechanism with exponential backoff
- Dead Letter Queue (DLQ) for jobs that keep failing
- Persistent storage (so jobs don’t get lost when restarting)

---

## ✨ Features

- Add jobs using simple CLI commands
- Run workers that continuously process queued jobs
- Automatic retries when a job fails
- Retry delay grows exponentially (1s → 2s → 4s → 8s…)
- If retries exceed limit → job goes to **Dead Letter Queue**
- Can manually retry jobs from DLQ
- Every job’s output (stdout/stderr) is saved and viewable later
- Uses SQLite so nothing extra needs to be installed

---

## 🛠️ Tech Used

- Python
- Typer (for CLI)
- SQLite (for storing jobs and logs)
- Multiprocessing (for running multiple workers)

No external message brokers like Redis or RabbitMQ.  
This project runs fully locally.

---

## 🚀 Getting Started

### 1. Install dependency:
pip install typer

### 2. Initialize the database:
python queuectl.py init

### 3. Add a job:
python queuectl.py enqueue "{"id":"job1","command":"echo Hello World"}"

### 4. Start a worker:
python queuectl.py worker start --count 1

(Press `Ctrl + C` to stop the worker.)

---

## ✅ Example: Successful Job

python queuectl.py enqueue "{"id":"hello","command":"echo Hi"}"
python queuectl.py worker start --count 1
python queuectl.py status
python queuectl.py logs hello

---

## ❌ Example: Job that Fails → Goes to DLQ

python queuectl.py enqueue "{"id":"bad","command":"bash -c 'exit 2'"}"
python queuectl.py worker start --count 1

Check dead jobs:

python queuectl.py dlq list

Retry it:

python queuectl.py dlq retry bad

---

## 🧠 How It Works (Simple Summary)

1. Jobs are stored in an SQLite database.
2. Workers continuously look for pending jobs.
3. A worker "claims" one job so no other worker touches it.
4. If the command runs successfully → state becomes `completed`.
5. If it fails → retry happens with exponential backoff.
6. If retries exceed limit → job moves to **Dead Letter Queue**.
7. DLQ jobs can be inspected or retried manually.

---

## 🎥 Demo Video (to be added)

Demo Link: 

---

## ✍️ Author

Made by **Vyasmarees**  
For FLam Backend Internship Assignment.