import logging

import pandas as pd
from cag.utils import config
from ..utils import utils
from ..nodes import WikiArticleRevision, WikiArticle
from cag.framework.creator.base_creator import GraphCreatorBase

import glob
from datetime import datetime

class WikipediaGraphCreator(GraphCreatorBase):

    #### Constant NODE and Edge NAMES  ####
    _WIKI_REVISION_NODE_NAME = WikiArticleRevision.__name__
    _WIKI_ARTICLE_NODE_NAME = WikiArticle.__name__

    
    _name = "Wikipedia Graph Creator"
    _description = "Creates a graph based on a Wikipedia Corpus with Revisions"

    #### Define Topology ####
    _edge_definitions = [
        {
            'relation' : GraphCreatorBase._BELONGS_TO_RELATION_NAME,
            'from_collections': [GraphCreatorBase._TEXT_NODE_NAME,
                                 _WIKI_REVISION_NODE_NAME,
                                 _WIKI_ARTICLE_NODE_NAME,
                                 GraphCreatorBase._IMAGE_NODE_NAME],
            'to_collections': [_WIKI_REVISION_NODE_NAME,
                               _WIKI_ARTICLE_NODE_NAME,
                               GraphCreatorBase._CORPUS_NODE_NAME]
        },
        {
            'relation' : GraphCreatorBase._REFERS_TO_RELATION_NAME,
            'from_collections': [_WIKI_REVISION_NODE_NAME],
            'to_collections': [_WIKI_REVISION_NODE_NAME, GraphCreatorBase._WEB_RESOURCE_NODE_NAME]
        },
        {
            'relation' : GraphCreatorBase._HAS_AUTHOR_RELATION_NAME,
            'from_collections': [_WIKI_REVISION_NODE_NAME],
            'to_collections': [GraphCreatorBase._AUTHOR_NODE_NAME]
        }
    ]



    #########################################################
    ### Main functions to initialize and update the graph ###
    #########################################################
    def init_graph(self):
        # get all articles - process each article by itself
        wikipedia_pages_paths = glob.glob(f"{self.corpus_file_or_dir}/*.parquet")
        self.latest_update = None
        if len(wikipedia_pages_paths) == 0:
            print("The wikipedia_tools download wikipedia pages revisions for a period of time. It downloads"
                  "a parquet file for each wikipedia page revisions for a period of time. ")
            return

        print('there are {} wiki titles'.format(len(wikipedia_pages_paths)))
        self._set_corpus_vertex()
        for wiki_revisions_file in wikipedia_pages_paths:
            orig_df = pd.read_parquet(wiki_revisions_file)
            self._create_update_wiki_article(orig_df)

        if self.latest_update is not None:
            self._set_corpus_vertex(self.latest_update)


    def update_graph(self, timestamp:datetime=None):
        self.init_graph()


    def create_wiki_article_vertex(self, title, timestamp=None):

        dict_ = {
            "name": title,
            "timestamp": timestamp
        }
        unique_by = ["name"]

        wiki_article = self.upsert_node(WikipediaGraphCreator._WIKI_ARTICLE_NODE_NAME,
                                        dict_, unique_by )

        return wiki_article

    def create_wiki_revision_vertex(self, revision_id, revision_timestamp):

        dict_ = {
            "rev_id": revision_id,
            "rev_timestamp": revision_timestamp,
            "timestamp": revision_timestamp
        }

        unique_by = "rev_id"

        wiki_rev = self.upsert_node(WikipediaGraphCreator._WIKI_REVISION_NODE_NAME, dict_, alt_key=unique_by)
        return wiki_rev



    #########################################
    ### End of Helpers to create vertices ###
    #########################################


    ### CREATE UPDATE 1 WIKI ARticle
    def _set_corpus_vertex(self, timestamp=None):
        """"
        The wiki revisions graph has one core vertex, the corpus vertex. Each wiki article belongs to it
        """
        self.corpus_vertex = self.create_corpus_vertex(key="WikiClimateChange", name=WikipediaGraphCreator._name,
                             type="social_media",
                             desc=WikipediaGraphCreator._description,
                             created_on=self.now,
                            timestamp= timestamp)

    ### Main method to create and update a wiki article
    def _create_update_wiki_article(self, orig_df):
        _df = orig_df.copy()

        _df['timestamp_str'] = _df['timestamp']
        _df['timestamp'] = pd.to_datetime(_df['timestamp'], infer_datetime_format=True)
        _df = _df.sort_values(by=['timestamp'])
        ## We assume that the wiki data is entered incrementally based on time - otherwise the logic if "latest_update" must be changed
        self.latest_update = _df['timestamp'].max() if (_df['timestamp'].max() > self.latest_update or self.latest_update is None) else self.latest_update
        wiki_article = self.create_wiki_article_vertex(orig_df['page'].values[0],
                                                       {"timestamp": _df['timestamp'].max()})
        self.upsert_edge(
            GraphCreatorBase._BELONGS_TO_RELATION_NAME,
            wiki_article,
            self.corpus_vertex,
            {"timestamp": _df['timestamp'].max()}
        )

        for idx, revision in _df.iterrows():
            wiki_rev_vertex = self.create_wiki_revision_vertex(
                revision['page'] + revision['timestamp_str'],
                revision['timestamp']
            )

            self.upsert_edge(
                GraphCreatorBase._BELONGS_TO_RELATION_NAME,
                wiki_rev_vertex,
                wiki_article,
                {"timestamp": revision['timestamp']}
            )

            text_vertex = self.create_text_vertex(revision['content'])
            self.upsert_edge(
                GraphCreatorBase._BELONGS_TO_RELATION_NAME,
                text_vertex,
                wiki_rev_vertex,
                {"timestamp": revision['timestamp']}
            )

            for img in revision['images']:
                img_vertex = self.create_image_vertex(img)
                self.upsert_edge(
                    GraphCreatorBase._REFERS_TO_RELATION_NAME,
                    wiki_rev_vertex,
                    img_vertex,
                    {"timestamp": revision['timestamp']}
                )

            for web_resource in revision['urls']:
                web_resource_vertex = self.create_web_resource_vertex(web_resource)
                self.upsert_edge(
                    GraphCreatorBase._REFERS_TO_RELATION_NAME,
                    wiki_rev_vertex,
                    web_resource_vertex,
                    {"timestamp": revision['timestamp']}
                )

            author_vertex = self.create_author_vertex(revision['user'])
            self.upsert_edge(
                GraphCreatorBase._HAS_AUTHOR_RELATION_NAME,
                wiki_rev_vertex,
                author_vertex,
                {"timestamp": revision['timestamp']}
            )
        return self.graph





