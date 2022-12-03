from wikipedia_tools.scraper import downloader
from datetime import datetime



def download_data():
    root_folder = r"C:\Users\elba_ro\Documents\projects\cag_wikipedia_usecase"

    categories = ["Climate_change", "Artificial_intelligence"]
    october_2022 = (datetime(year=2022, month=10, day=1), datetime(year=2022, month=10, day=31))
    october_2012 = (datetime(year=2012, month=10, day=1), datetime(year=2012, month=10, day=31))
    from_to_rev_periods = [october_2022, october_2012]

    for category in categories:
        for from_, to_ in from_to_rev_periods:
            wiki_downloader = downloader.WikiPagesRevision(
                categories=[category],
                revisions_from=from_,
                revisions_to=to_,
                save_each_page=True,
                category_depth=1,
                out_version="parquet",
                root_folder= root_folder
                )
            wiki_downloader.download()



        
if __name__ == "__main__":
    download_data()