from abc import ABC, abstractmethod
import queue
import os
import multiprocessing
from math import ceil

from src.reader import BaseReader
from src.logger import logger


class CTGBaseAnalyzer(ABC):
    def __init__(
        self,
        directory: str,
        reader: BaseReader,
        processes_count: int = 8,
    ):
        self.directory = directory
        self.reader = reader
        self.processes_count = processes_count
        self.data_queue = multiprocessing.Queue()
        self.event = multiprocessing.Event()

    def filling_queue(self):
        files = os.listdir(self.directory)
        files_count = len(files)
        process_files = ceil(files_count / self.processes_count)

        processes = [
            multiprocessing.Process(
                target=self.read_file,
                args=[
                    files[
                        border
                        * process_files : min(files_count, (border + 1) * process_files)
                    ]
                ],
                daemon=True,
            )
            for border in range(self.processes_count)
        ]

        for process in processes:
            process.start()
        logger.info("File reading processes started")

        for process in processes:
            process.join()
        logger.info("File read processes have finished executing")

        self.data_queue.put({"file": "end_of_files"})

    def read_file(self, files):
        reader = self.reader()

        for file in files:
            _file = os.path.join(self.directory, file)

            if not os.path.isfile(_file):
                logger.warning(f"{_file} is not а file")
                break

            data = reader.read(_file)

            if data is not None:
                self.data_queue.put({"file": file, "ctg": data})
            else:
                logger.warning(f"Data of file: {_file} is None")

            logger.debug(f"File: {_file} read")

    def work(self):
        parent_conn, child_conn = multiprocessing.Pipe()

        processes = [
            multiprocessing.Process(target=self.analyze, args=[child_conn], daemon=True)
            for _ in range(self.processes_count)
        ]

        for process in processes:
            process.start()
        logger.info("Working processes started")

        self.filling_queue()

        for process in processes:
            process.join()
        logger.info("Working processes have finished executing")

        result_dict = {}

        while True:
            result = parent_conn.recv()
            if result == "end_of_files":
                logger.debug("'end_of_files' line read from pipe")
                break

            result = result.split(":")
            result_dict[result[0]] = result[1]

        return result_dict

    @abstractmethod
    def analyze(self):
        pass


class CTGFisherAnalyzer(CTGBaseAnalyzer):
    def __init__(
        self,
        directory: str,
        reader: BaseReader,
        processes_count: int = 8,
    ):
        super().__init__(directory, reader, processes_count)

        self.basal_rhythm = None
        self.amplitude = None
        self.variability = None
        self.acceleration = None
        self.decelerations = None

    def analyze(self, pipe: multiprocessing.Pipe):
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

                pipe.send("end_of_files")

                break

            logger.debug(f"processed {data['file']=}")

            pipe.send(f"{data['file']}:хорошее")
