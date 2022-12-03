
import config
from creators.wikipedia_graph_creator import WikipediaGraphCreator
from annotators.pipeline.textnodes_pipeline import CagWikipedaPipeline
from cag.utils.timer import Timer
def create_graph():
    graph_creator = WikipediaGraphCreator(
                            corpus_file_or_dir="", 
                            conf = config.Config, 
                            initialize=True, 
                            load_generic_graph=False
                            )

def annotate_graph():
    my_first_pipeline = CagWikipedaPipeline(config.Config, batch_size =1000)
    my_first_pipeline.init_and_run()

if __name__ == "__main__":
    timer = Timer()
    timer.start()

    create_graph()
    timer.stop()

    annotate_graph()
    timer.stop()