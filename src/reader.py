import ast
import pandas as pd
from abc import ABC, abstractmethod

from src.logger import logger


class BaseReader(ABC):
    @abstractmethod
    def read(self, filename):
        pass


class DictReader(BaseReader):
    def read(self, filename: str) -> pd.DataFrame:
        with open(filename, "r") as file:
            logger.info(f"File {filename} open")

            graph_list = ast.literal_eval(file.read())

            x_coords = [coords.get("Key") for coords in graph_list]
            y_coords = [coords.get("Value") for coords in graph_list]
            coords = pd.DataFrame.from_dict(
                {
                    "x": x_coords,
                    "y": y_coords,
                }
            )
            logger.info(f"File {filename} readed")

            return coords
