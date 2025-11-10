from abc import ABC, abstractmethod
from webbrowser import get


class VectorDBInterface(ABC):

        @abstractmethod
        def connect(self):
            pass

        @abstractmethod
        def disconnect(self):
            pass

        @abstractmethod
        def is_collection_exists(self, collection_name: str) -> bool:
            pass

        @abstractmethod
        def get_all_collections(self) -> list:
            pass

        @abstractmethod
        def get_collection_info(self, collection_name: str) -> dict:
            pass

        @abstractmethod
        def delete_collection(self, collection_name: str):
            pass

        @abstractmethod
        def create_collection(self, collection_name: str, embading_size: int, do_rest: bool = False):
            pass

        
