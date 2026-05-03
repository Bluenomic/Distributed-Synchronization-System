import subprocess
import os
import time
import logging

# Configuration
LOCUST_FILE = "benchmarks/load_test_scenarios.py"
OUTPUT_DIR = "docs/benchmarks"
DURATION = "1m"  # Run for 1 minute
USERS = 10
SPAWN_RATE = 2
TARGET_URL = "http://localhost:8001" # Target Node 1 (Lock Manager)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BenchmarkRunner")

def run_benchmark():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logger.info(f"Created directory: {OUTPUT_DIR}")

    timestamp = int(time.time())
    csv_prefix = os.path.join(OUTPUT_DIR, f"result_{timestamp}")

    logger.info(f"Starting Locust benchmark against {TARGET_URL}...")
    logger.info(f"Users: {USERS}, Spawn Rate: {SPAWN_RATE}, Duration: {DURATION}")

    # Command to run Locust in headless mode
    command = [
        "locust",
        "-f", LOCUST_FILE,
        "--headless",
        "-u", str(USERS),
        "-r", str(SPAWN_RATE),
        "--run-time", DURATION,
        "--host", TARGET_URL,
        "--csv", csv_prefix,
        "--only-summary"
    ]

    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            logger.info("Benchmark completed successfully!")
            logger.info(f"Results saved with prefix: {csv_prefix}")
            # List generated files
            files = [f for f in os.listdir(OUTPUT_DIR) if str(timestamp) in f]
            for f in files:
                logger.info(f" - {f}")
        else:
            logger.error(f"Benchmark failed with return code {process.returncode}")
            logger.error(stderr)

    except Exception as e:
        logger.error(f"An error occurred while running benchmark: {e}")

if __name__ == "__main__":
    # Check if locust is installed
    try:
        subprocess.run(["locust", "-V"], check=True, capture_output=True)
        run_benchmark()
    except Exception:
        logger.error("Locust is not installed or not in PATH. Please run 'pip install locust' first.")
