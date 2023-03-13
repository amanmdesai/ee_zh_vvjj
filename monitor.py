import psutil
import subprocess
import time
import matplotlib.pyplot as plt

# List the script names to be monitored
# script_names = ["p8_ee_WW_ecm240", "p8_ee_WW_ecm240_score", "p8_ee_WW_ecm240_score2"]
script_names = ["p8_ee_WW_ecm240", "p8_ee_WW_ecm240_score", "p8_ee_WW_ecm240_score2"]

# Create empty lists to store memory usage for each script
mem_usages = [[] for _ in range(len(script_names))]
times = [[] for _ in range(len(script_names))]

# Loop through each script name and start a subprocess for each one
for i, script_name in enumerate(script_names):
    # Start the subprocess
    p = subprocess.Popen(["python", "minimal.py", script_name])

    start_time = time.time()

    while True:
        # Get the memory usage of the subprocess
        mem_info = psutil.Process(p.pid).memory_info()

        # Append the memory usage to the appropriate list
        mem_usages[i].append(mem_info.rss / (1024 * 1024 * 1024))
        times[i].append(time.time() - start_time)

        # Wait for 1 second before checking again
        time.sleep(1)

        # If the subprocess has finished, break out of the loop
        if p.poll() is not None:
            break

# Plot the memory usage of each script over time on the same plot
for i, script_name in enumerate(script_names):
    print(
        script_name,
        times,
    )
    plt.plot(times[i], mem_usages[i], label=script_name)

plt.xlabel("Time (s)")
plt.ylabel("Memory usage (GB)")
plt.legend()
plt.yscale("log")
plt.savefig("memory_usage.png")
