
from math import log
import re
from ..VectorDBInterface import VectorDBInterface
from ..VectorDBEnums import VectorDBEnums, DistanceMethodEnums
from qdrant_client import QdrantClient, models
import logging
from typing import List
class QdrantDBProvider(VectorDBInterface):
    def __init__(self,db_path: str ,destance_method: str):
        self.client = None
        self.db_path = db_path
        self.destance_method = destance_method

        if destance_method == DistanceMethodEnums.COSINE.value:
            self.distance = models.Distance.COSINE
        elif destance_method == DistanceMethodEnums.DOT.value:
            self.distance = models.Distance.DOT

        self.logger = logging.getLogger(__name__)

    def connect(self):
        self.client = QdrantClient(path=self.db_path)
        self.logger.info("Connected to QdrantDB at %s", self.db_path)
    
    def disconnect(self):
        self.client = None
        self.logger.info("Disconnected from QdrantDB")

    def is_collection_exists(self, collection_name: str) -> bool:
        return self.client.collection_exists(collection_name=collection_name)
    
    def get_all_collections(self) -> List:
        return self.client.get_collections()
    
    def get_collection_info(self, collection_name: str) -> dict:
        return self.client.get_collection(collection_name=collection_name)
    
    def delete_collection(self, collection_name: str):
        if self.is_collection_exists(collection_name):
            self.client.delete_collection(collection_name)
            self.logger.info("Collection %s deleted", collection_name)

    def create_collection(self, collection_name: str, embading_size: int, do_rest: bool = False):
        if do_rest:
            _=self.delete_collection(collection_name)
        
        if not self.is_collection_exists(collection_name):
            _=self.client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=embading_size,
                    distance=self.distance
                )
            )
            self.logger.info("Collection %s created with embedding size %d", collection_name, embading_size)
            return True
        return False
    
    def insert_one(self, collection_name: str, text: str,vector:list,
                              metadata: dict=None, record_id: str = None):
        if not self.is_collection_exists(collection_name):
            self.logger.error("Collection %s does not exist", collection_name)
        
            return False
        try:
            _=self.client.upload_records(
                collection_name=collection_name,
                records=[
                    models.Record(
                        vector=vector,
                        payload=metadata
                    )
                    ]   
                )
        except Exception as e:
            self.logger.error("Error inserting record: %s", str(e))
            return False
        return True
        
    def insert_many(self, collection_name: str, texts: list,vectors:list,
                       metadata: list=None, record_ids: list  = None, batch_size: int = 50):
        

        if metadata is None:
            metadata = [None] * len(texts)
        if record_ids is None:
            record_ids = [None] * len(texts)

        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:batch_size]
            batch_vectors = vectors[i:batch_size]
            batch_metadata = metadata[i:batch_size]
        
            records = [
                models.Record(
                    vector=batch_vectors[x],
                    payload={
                        "text": batch_texts[x],
                        "metadata": batch_metadata[x]
                    }
                )
                for x in range(len(batch_texts))
            ]
            try:
                _=self.client.upload_records(
                    collection_name=collection_name,
                    records=records
                )
            except Exception as e:
                self.logger.error("Error inserting batch starting at index  %s", str(e))
                return False
        return True
    def search_by_vector(self, collection_name: str, vector: list, limit: int=5):
        return self.client.search(
            collection_name=collection_name,
            query_vector=vector,
            limit=limit
        )
