import os

from cag.framework.annotator.pipeline import Pipeline
from pyArango.collection import Collection

from ...config import Config
from tqdm import tqdm

class CagWikipedaPipeline(Pipeline):

    def __init__(self, database_config: Config, batch_size:int = 200):
        super().__init__(database_config)
        self.period = None
        self.batch_size = batch_size


    def process_input(self) -> list:
        processed = []
        for txt_node in tqdm(self.input):
            processed.append((txt_node.text, {"_key": txt_node._key}))

        return processed

    def init_and_run(self) -> list:


        # 3. Loop over your data, annotate and save

        coll: Collection = self.database_config.db["TextNode"]
        docs = coll.fetchAll(limit=self.batch_size)
        fetched = len(docs)

        # set the pipeline by adding pre defined pipelines from CAG or using cunstomized ones
        self.add_annotation_pipe(name="NamedEntityPipeOrchestrator", save_output=True, is_spacy=True) # spacy 
        self.add_annotation_pipe(name="EmpathPipeOrchestrator", save_output=True, is_spacy=True)

        while docs != None and len(docs) >0:
            ## annotate


            # Set the INPUT - this will automatically call preprocess_input (make sure to implement it)
            self.set_input(docs)
            self.annotate()

            self.save()
            self.reset_input_output()
            print(f"Processed {fetched} docs")
            docs = coll.fetchAll(limit=self.batch_size, skip=fetched)
            fetched = fetched + len(docs)
