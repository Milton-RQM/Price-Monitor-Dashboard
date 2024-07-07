import scrapy
from scrapy.item import Field, Item
from scrapy.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from scrapy.linkextractors import LinkExtractor
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess
from itemloaders.processors import MapCompose
from datetime import datetime
import os

class Productos_Tia(Item):

    Producto = Field()
    Nombre = Field()
    Precio = Field()
    FechaConsulta = Field()
    Categoria = Field()
    LinkPrincipal = Field()
    ValorScrapy = Field()

class TiaSpider(CrawlSpider):

    name = 'Tia'
    custom_settings = {
        'USER_AGENT': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        'CLOSESPIDER_PAGECOUNT': 10,
        'ROBOTSTXT_OBEY': False,
        'REDIRECT_ENABLED': True,
        'LOG_LEVEL': 'DEBUG'

    }

    allowed_domains = ['tia.com.ec']
    # Define un diccionario de grupos de categorías

    category_groups = {
        'lacteos': [
            'huevos',
            'quesos',
            'crema-de-leche'  # Agrega las demás categorías aquí
        ],
        'comestibles': [
            'aceites-y-vinagres'  # Agrega las demás categorías aquí
        ],
        'aguas-y-bebidas': [
            'bebidas-gaseosas-y-maltas'  # Agrega las demás categorías aquí
        ]
    }

    download_delay = 1

    def start_requests(self):

        base_url = 'https://www.tia.com.ec/supermercado/'
        for group, categories in self.category_groups.items():
            for category in categories:
                url = f'{base_url}{group}/{category}/'
                meta = {
                    'categoria': category,
                    'grupo': group,
                    'link_principal': url,
                    'fecha_consulta': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                yield scrapy.Request(url=url, callback=self.parse_tia, meta=meta)

    rules = (
        Rule(
            LinkExtractor(
                allow=r'p=',
                tags=('a', 'button'),
                attrs=('href', 'data-url')
            ), follow=True, callback='parse_tia'),
    )

    def quitarSimboloDolar(self, texto):
        nuevoTexto = texto.replace("$", "")
        nuevoTexto = nuevoTexto.replace('\n', '').replace('\r', '').replace('\t', '')
        return nuevoTexto

    def parse_tia(self, response):
        sel = Selector(response)
        productos = sel.xpath('//div[@class="column main"]//div[@class="products wrapper grid products-grid"]')

        for producto in productos:
            item = ItemLoader(Productos_Tia(), producto)
            #item.add_xpath('Producto', './/div[@class="tile-body px-3 pt-3 pb-0 d-flex flex-column pb-2"]/a[@class="product-brand text-uppercase m-0"]/text()', MapCompose(lambda i: i.replace('\n', '').replace('\r', '')))
            #item.add_xpath('Nombre', './/div[@class="tile-body px-3 pt-3 pb-0 d-flex flex-column pb-2"]//div[@class="pdp-link"]/a/text()', MapCompose(lambda i: i.replace('\n', '').replace('\r', '')))
            item.add_xpath('Precio', './/li[@class="item product product-item"]//div[@class="price-box-normal price-box price-final_price"]//span[@class="price"]/text()', MapCompose(self.quitarSimboloDolar))
            item.add_value('FechaConsulta', response.meta['fecha_consulta'])
            item.add_value('Categoria', response.meta['categoria'])
            item.add_value('LinkPrincipal', response.meta['link_principal'])
            item.add_value('ValorScrapy', 'Tia')
            yield item.load_item()

filename = f"data_tia_{datetime.now().strftime('%Y-%m-%d')}.csv"

if os.path.exists(filename):
    os.remove(filename)
# EJECUCION

process = CrawlerProcess({

    'FEED_FORMAT': 'csv',
    'FEED_URI': 'output/' + filename

})

print("Iniciando proceso")
process.crawl(TiaSpider)
process.start()

