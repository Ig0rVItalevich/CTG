import pickle
import argparse
import logging

from src.analyzer import CTGFisherAnalyzer
from src.vizualizer import CTGVisualizer
from src.reader import DictReader
from src.compare_results import compare_results
from src.logger import logger

expected_result = None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--dir", default="./ctg_files")
    parser.add_argument("-t", "--threads", default=8)
    parser.add_argument("-visualize", action="store_const", const=True)

    args = parser.parse_args()

    if args.visualize:
        visualizer = CTGVisualizer(
            directory=args.dir, reader=DictReader, processes_count=int(args.threads)
        )

        visualizer.work()
    else:
        analyzer = CTGFisherAnalyzer(
            directory=args.dir, reader=DictReader, processes_count=int(args.threads)
        )

        with open("expected_result.txt", "rb") as file:
            expected_result = pickle.load(file)

        calculated_result = analyzer.work()

        compare_results(expected_result, calculated_result)
