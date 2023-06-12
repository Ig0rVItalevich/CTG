import queue
import os
import shutil
import multiprocessing

import matplotlib.pyplot as plt

from src.analyzer import CTGBaseAnalyzer
from src.reader import BaseReader
from src.logger import logger


class CTGVisualizer(CTGBaseAnalyzer):
    def __init__(self, directory: str, reader: BaseReader, processes_count: int = 8):
        super().__init__(directory, reader, processes_count)

    def check_directory(self):
        graphs_directory = "./graphs"

        if os.path.exists(graphs_directory):
            shutil.rmtree(graphs_directory)
            logger.info(f"Directory {graphs_directory} exists and cleared")

        os.mkdir(graphs_directory)
        logger.debug(f"Created directory {graphs_directory}")

    def work(self):
        self.check_directory()

        processes = [
            multiprocessing.Process(target=self.vizualize, daemon=True)
            for _ in range(self.processes_count)
        ]

        for process in processes:
            process.start()
        logger.info("Working processes started")

        self.filling_queue()

        for process in processes:
            process.join()
        logger.info("Working processes have finished executing")

    def vizualize(self):
        while not self.event.is_set():
            try:
                data = self.data_queue.get(timeout=1)
            except queue.Empty:
                logger.warning("Received an exception 'queue.Empty'")
                continue

            if data["file"] == "end_of_files":
                logger.info("'end_of_files' line read from queue")

                self.event.set()
                logger.info("self.event is set")

                break

            ctg = data["ctg"]
            file_index = int(data["file"][:-4])

            plt.figure(file_index)
            plt.rcParams["figure.figsize"] = [100, 10]
            plt.xlim(0, 2500)
            plt.xticks(range(0, 2500, 30))
            plt.ylim(50, 200)
            plt.grid(which="major")
            plt.grid(which="minor")

            plt.plot(ctg["x"], ctg["y"])
            plt.savefig(f"graphs/{file_index}.png")

            logger.info(f"Graph {file_index} saved at graphs/{file_index}.png")

            logger.debug(f"vizualized {data['file']}")

    def analyze(self):
        pass
