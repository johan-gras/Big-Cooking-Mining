import scrapy 
from scrapy.item import Item, Field 
from scrapy.selector import Selector 
from scrapy.http import HtmlResponse


class SpiderSelection(scrapy.Spider):
    # nom du Scraper
    name = "scrap_marmiton3" 
    # les dommaines autorises
    allowed_domains = ["marmition.org"] 
    # Les urls ou notre scraper va commencer a scraper 
    start_urls = ["http://www.marmiton.org/recettes/selections.aspx"]
    
    # c'est de cette methode que notre scrapper va commencer son travaille
    def parse(self, response):
        i = 0
        # pour tout lien trouver dans la classe m_liste_produits et qui possede des <li> on extrais le lien
        for href in response.css(".m_liste_produits li a::attr(href)"):
            # l'affecte a url
            url = response.urljoin(href.extract())
            i += 1
            print (url)
            yield
        print (i)
        
