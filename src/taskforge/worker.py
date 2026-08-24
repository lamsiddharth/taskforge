from taskforge.queue import TaskQueue
from taskforge.models import Task

def main() -> None:
    task_queue = TaskQueue()    
    print("Worker started. Waiting for tasks... hnn re maghia")
    while(True):
        task = task_queue.pull()
        if task:
            # Process the task
            print(f"Processing task: {task}")

if __name__ == "__main__":
    main()