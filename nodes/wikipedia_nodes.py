from cag.graph_elements.nodes  import GenericOOSNode,  Field

"""OOS NODES RELEVANT FOR WIKIPEDIA CORPUS"""


class WikiArticle(GenericOOSNode):
    _fields = {
        "name": Field(),
        "lang": Field(),
        "is_popular": Field(),
        "is_important": Field(),
        **GenericOOSNode._fields
    }


class WikiArticleRevision(GenericOOSNode):
    _fields = {
        'rev_id': Field(),
        'rev_timestamp': Field(),
        **GenericOOSNode._fields
    }


class WikiArticleSection(GenericOOSNode):
    _fields = {
        'name': Field(),
        **GenericOOSNode._fields
    }




