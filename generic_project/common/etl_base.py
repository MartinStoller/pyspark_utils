from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class ETLJob(ABC):
    _allowed_methods = {
        "extract",
        "transform",
        "load",
        "run",
        "__init__",
        "__module__",
        "__doc__",
        "__static_attributes__",
        "__firstlineno__",
    }

    def __init_subclass__(cls):
        """
        prevents the implementation of this class from defining additional functions, as those should
        be defined in the modules transformations.py or the common or shared module.
        """
        super().__init_subclass__()
        extra = set(cls.__dict__) - cls._allowed_methods
        if extra:
            raise TypeError(f"{cls.__name__} defines extra methods: {extra}")

    @abstractmethod
    def extract(self) -> DataFrame | tuple[DataFrame, ...]:
        """
        Responsible for Loading the input data
        """
        pass

    @abstractmethod
    def transform(self, *inputs: DataFrame) -> DataFrame:
        """
        All Business Logic goes here.
        """
        pass

    @abstractmethod
    def load(self, output: DataFrame) -> None:
        """
        Write results to target.
        """
        pass

    @abstractmethod
    def run(self) -> None:
        """
        Main function. Mainly orchestrates the extract → transform → load steps.
        """
        pass
