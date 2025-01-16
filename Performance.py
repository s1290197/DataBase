import time
import pandas as pd
import os
import psutil
import matplotlib.pyplot as plt

class Performance:
    """
    A class to measure, record, and visualize the performance of various database operations,
    including insertion, reading, and deletion. It also integrates external results (e.g., Apache Ignite).

    Attributes:
        db_instances (list): A list of database instances to evaluate.
        mode (str): Output mode, either "plot" for visualization or "save" to store results in CSV files.
        operation_times (dict): Stores execution times for each operation.
        operation_uss (dict): Stores USS (Unique Set Size) memory usage for each operation.
        operation_rss (dict): Stores RSS (Resident Set Size) memory usage for each operation.
    """

    def __init__(self, db_instances, mode="plot"):
        """
        Initializes the Performance class with database instances and output mode.

        Args:
            db_instances (list): List of database instances.
            mode (str): Output mode, either "plot" or "save" (default is "plot").
        """
        self.db_instances = db_instances
        self.mode = mode
        self.operation_times = {
            "insert_5min": {},
            "insert_1hour": {},
            "read_5min": {},
            "read_1hour": {},
            "delete": {}
        }
        self.operation_uss = {
            "insert_5min": {},
            "insert_1hour": {},
            "read_5min": {},
            "read_1hour": {},
            "delete": {}
        }
        self.operation_rss = {
            "insert_5min": {},
            "insert_1hour": {},
            "read_5min": {},
            "read_1hour": {},
            "delete": {}
        }

    def load_ignite_results(self, file_paths):
        """
        Loads Apache Ignite results from CSV files and organizes them into a structured format.

        Args:
            file_paths (list): List of file paths to Apache Ignite result files.

        Returns:
            dict: Processed Ignite results organized by operation names.
        """
        ignite_results = {}
        operation_mapping = {
            "1hour": "insert_1hour",
            "5min_result.csv": "insert_5min"
        }
        for file_path in file_paths:
            results_df = pd.read_csv(file_path)
            for _, row in results_df.iterrows():
                operation_name = operation_mapping.get(row['Operation'], row['Operation'])
                ignite_results[operation_name] = {
                    "Time (s)": row['Time (ms)'] / 1000,
                    "USS (MB)": row['USS (KB)'] / 1024,
                    "RSS (MB)": row['RSS (KB)'] / 1024
                }
        return ignite_results

    def integrate_ignite_results(self, ignite_results):
        """
        Integrates Apache Ignite results into the class's performance metrics.

        Args:
            ignite_results (dict): Ignite performance results.
        """
        for operation_name, values in ignite_results.items():
            self.operation_times[operation_name]["Ignite"] = values["Time (s)"]
            self.operation_uss[operation_name]["Ignite"] = values["USS (MB)"]
            self.operation_rss[operation_name]["Ignite"] = values["RSS (MB)"]

    def measure_external_memory(self):
        """
        Measures the external memory usage (USS and RSS) of the current process.

        Returns:
            tuple: RSS and USS memory usage in MB.
        """
        process = psutil.Process(os.getpid())
        memory_info = process.memory_full_info()
        return memory_info.rss / (1024 ** 2), memory_info.uss / (1024 ** 2)

    def measure_performance(self, db, operation_name, *args):
        """
        Measures the execution time and memory usage for a specified database operation.

        Args:
            db (object): Database instance.
            operation_name (str): Name of the operation (e.g., "insert_5min").
            *args: Additional arguments for the operation.
        """
        start_time = time.time()
        getattr(db, operation_name)(*args)
        end_time = time.time()

        db_name = type(db).__name__
        self.operation_times[operation_name][db_name] = end_time - start_time
        rss, uss = self.measure_external_memory()
        self.operation_uss[operation_name][db_name] = uss
        self.operation_rss[operation_name][db_name] = rss

    def summarize_results(self):
        """
        Prints a summary of execution times and memory usage for all operations.
        """
        print("Execution Times (seconds):")
        for operation, db_times in self.operation_times.items():
            print(f"{operation}: {db_times}")

        print("\nUSS (MB):")
        for operation, db_uss in self.operation_uss.items():
            print(f"{operation}: {db_uss}")

        print("\nRSS (MB):")
        for operation, db_rss in self.operation_rss.items():
            print(f"{operation}: {db_rss}")

    def plot_results(self, operation_name, title):
        """
        Plots execution time comparison for a specified operation across databases.

        Args:
            operation_name (str): Name of the operation to plot.
            title (str): Title of the plot.
        """
        db_names = list(self.operation_times[operation_name].keys())
        times = list(self.operation_times[operation_name].values())
        plt.figure(figsize=(10, 5))
        plt.bar(db_names, times, color="skyblue")
        plt.xlabel("Database")
        plt.ylabel("Time (seconds)")
        plt.title(f"{title} Performance Comparison")
        plt.show()

    def save_results_to_csv(self, output_dir="results"):
        """
        Saves performance metrics to CSV files in a specified directory.

        Args:
            output_dir (str): Directory to save the CSV files (default is "results").
        """
        os.makedirs(output_dir, exist_ok=True)
        pd.DataFrame(self.operation_times).transpose().to_csv(os.path.join(output_dir, "execution_times.csv"))
        pd.DataFrame(self.operation_uss).transpose().to_csv(os.path.join(output_dir, "memory_uss.csv"))
        pd.DataFrame(self.operation_rss).transpose().to_csv(os.path.join(output_dir, "memory_rss.csv"))

    def run(self, ignite_results_files, file_path_5min=None, file_path_1hour=None):
        """
        Executes the performance evaluation process for defined operations.

        Args:
            ignite_results_files (list): Paths to Ignite result files.
            file_path_5min (str): Path to the 5-minute dataset (optional).
            file_path_1hour (str): Path to the 1-hour dataset (optional).
        """
        ignite_results = self.load_ignite_results(ignite_results_files)
        self.integrate_ignite_results(ignite_results)
        
        for db in self.db_instances:
            for operation in ["insert_5min", "insert_1hour"]:
                file_path = file_path_5min if operation == "insert_5min" else file_path_1hour
                if file_path:
                    self.measure_performance(db, operation, file_path)

        if self.mode == "save":
            self.save_results_to_csv()
        else:
            self.summarize_results()
