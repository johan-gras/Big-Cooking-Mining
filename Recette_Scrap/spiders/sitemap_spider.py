from scrapy.spiders import SitemapSpider

class MySpider(SitemapSpider):
    name = "scrap_sitemap" 
    sitemap_urls = ['http://www.marmiton.org/wsitemap_1.xml']
    sitemap_rules = [
        ('/recettes/', 'parse_recettes'),
    ]

    def parse_recettes(self, response):
        #Time parsing
        time = {
            'total' : response.css(".title-2.recipe-infos__total-time__value::text").extract_first(),
            'preparation' : response.css(".recipe-infos__timmings__preparation .recipe-infos__timmings__value::text").extract_first().strip("\r\n\t"),
        }
        cooking = response.css(".recipe-infos__timmings__cooking .recipe-infos__timmings__value::text").extract_first()
        if cooking is not None:
            time['cooking'] = cooking.strip("\r\n\t")
        
        #Ingredients parsing
        ingredients_selector = response.css(".recipe-ingredients__list__item")
        ingredients = []
        for ingredient in ingredients_selector:
            dict = {'name' : ingredient.css(".ingredient::text").extract_first()}
            quantity = ingredient.css(".recipe-ingredient-qt::text").extract_first()
            if quantity is not None:
                dict['quantity'] = quantity
            ingredients.append(dict)
        
        #Utensils parsing
        utensils_selector = response.css(".recipe-utensil__name::text").extract()
        utensils = []
        for utensil in utensils_selector:
            ustensil = utensil.strip("\r\n\t")
            if ustensil != "":
                utensils.append(ustensil)
        
        #Etapes parsing
        etapes_selector = response.css(".recipe-preparation__list__item")
        etapes = []
        for i, etape in enumerate(etapes_selector):
            description = ""
            for string in etape.css("::text").extract():
                description += string.strip("\r\n\t")
            
            dict = {
                'Etape' : i + 1,
                'Description' : description.replace("Etape " + str(i+1), '', 1)
            }
            etapes.append(dict)
        
        #Main parsing
        scraped_recipe = {
            'url' : response.url,
            'title' : response.css("h1.main-title::text").extract_first(),
            
            'time' : time,
            'number_of_person' : response.css(".title-2.recipe-infos__quantity__value::text").extract_first(),
            'level' :  response.css(".recipe-infos__level").xpath("div/@class").re_first(".$"),
            'budget' :  response.css(".recipe-infos__budget").xpath("div/@class").re_first(".$"),
            
            'ingredients' : ingredients,
            'utensils' : utensils,
            'etapes' : etapes
        }
        
        #Rating scraping
        rating = response.css(".recipe-reviews-list__review__head__infos__rating__value::text").extract_first()
        if rating is not None:
            scraped_recipe['rating'] = rating
        
        yield scraped_recipe
        