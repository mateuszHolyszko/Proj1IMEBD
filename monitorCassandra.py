import docker
import time
from cassandra.cluster import Cluster

# Initialize Docker client
client = docker.from_env()

def calculate_cpu_percent(stats):
    cpu_delta = stats['cpu_stats']['cpu_usage']['total_usage'] - stats['precpu_stats']['cpu_usage']['total_usage']
    system_delta = stats['cpu_stats']['system_cpu_usage'] - stats['precpu_stats']['system_cpu_usage']
    if system_delta > 0.0 and cpu_delta > 0.0:
        return (cpu_delta / system_delta) * len(stats['cpu_stats']['cpu_usage']['percpu_usage']) * 100.0
    return 0.0

def log_stats(container_name, duration=60):
    container = client.containers.get(container_name)
    start_time = time.time()

    cpu_sum = 0.0
    memory_sum = 0.0
    samples = 0

    while time.time() - start_time < duration:
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

# Cassandra monitoring and query execution
def monitor_query(container_name):
    # Initialize Cassandra connection
    cluster = Cluster(['127.0.0.1'])  
    session = cluster.connect('my_keyspace')  

    # Execute query and time it
    start_time = time.time()
    session.execute("SELECT * FROM my_keyspace.games_recommended_by_users;")  
    end_time = time.time()

    query_duration = (end_time - start_time) * 1000  
    print(f"Query executed in {query_duration:.2f} ms.")

    # Run resource usage monitoring during query execution
    stats = log_stats(container_name, duration=query_duration / 1000)  

    return stats, query_duration

# Run the monitoring during query execution
stats, query_duration = monitor_query('cassandra10M')

print(f"Final resource usage: {stats}")
print(f"Query took {query_duration:.2f} ms.")
