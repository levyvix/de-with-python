import os
import signal
import subprocess

# Function to get the list of all airflow processes
def get_airflow_processes():
    result = subprocess.run(['ps', 'aux'], stdout=subprocess.PIPE)
    processes = result.stdout.decode().splitlines()
    airflow_processes = []
    
    for process in processes:
        if 'airflow' in process:
            pid = int(process.split()[1])
            airflow_processes.append(pid)
    
    return airflow_processes

# Function to kill the airflow processes
def kill_airflow_processes():
    airflow_processes = get_airflow_processes()
    
    for pid in airflow_processes:
        try:
            os.kill(pid, signal.SIGKILL)
            print(f"Process {pid} killed.")
        except ProcessLookupError:
            print(f"Process {pid} not found.")
        except Exception as e:
            print(f"Failed to kill process {pid}: {e}")

# Run the script to kill processes
kill_airflow_processes()
