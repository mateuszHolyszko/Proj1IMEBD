import docker
import time
import signal
import sys

# Initialize Docker client
client = docker.from_env()

# Flag to handle graceful exit
running = True

def signal_handler(sig, frame):
    global running
    print("\nExiting...")
    running = False

# Register signal handler for Ctrl+C
signal.signal(signal.SIGINT, signal_handler)

def calculate_cpu_percent(stats):
    
    cpu_delta = stats['cpu_stats']['cpu_usage']['total_usage'] - stats['precpu_stats']['cpu_usage']['total_usage']
    system_delta = stats['cpu_stats']['system_cpu_usage'] - stats['precpu_stats']['system_cpu_usage']
    if system_delta > 0.0 and cpu_delta > 0.0:
        return (cpu_delta / system_delta) * len(stats['cpu_stats']['cpu_usage']['percpu_usage']) * 100.0
    return 0.0

def log_stats(container_name, duration=60):
    
    global running
    container = client.containers.get(container_name)
    start_time = time.time()

    cpu_sum = 0.0
    memory_sum = 0.0
    samples = 0

    while running and time.time() - start_time < duration:
        stats = container.stats(stream=False)
        cpu = calculate_cpu_percent(stats)
        memory = stats['memory_stats']['usage'] / (1024 ** 3)  # Convert to GB

        cpu_sum += cpu
        memory_sum += memory
        samples += 1

        print(f"CPU: {cpu:.2f}%, RAM: {memory:.2f} GB")

    if samples == 0:  # Avoid division by zero
        return {"average_cpu": 0.0, "average_memory": 0.0}

    average_cpu = cpu_sum / samples
    average_memory = memory_sum / samples

    print(f"\nAverage CPU: {average_cpu:.2f}%, Average RAM: {average_memory:.2f} GB")
    return {"average_cpu": average_cpu, "average_memory": average_memory}

# Run the monitoring function
log_stats('postgres1M', duration=60)
