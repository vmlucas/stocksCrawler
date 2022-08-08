from json import load
import scrapy
import time

from ..items import CrawlerItem
from .DataLoad import loadData

class StockSpider(scrapy.Spider):
    name='yahooStocks'
    allowed_domains = ['finance.yahoo.com']
    start_urls = ['https://finance.yahoo.com/quote/AAPL']
    myHeaders={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}

    def start_requests(self):
        urls = ['https://finance.yahoo.com/most-active/']  # Most active Stocks start URL
        for url in urls:
            yield scrapy.Request(url=url, headers=self.myHeaders, callback=self.get_stocks)


    def get_stocks(self, response):
        # Get all the stock symbols
        stocks = response.xpath('//*[@id="scr-res-table"]/div[1]/table/tbody//tr/td[1]/a').css('::text').extract()
        for stock in stocks:
            print(stock)
            # Follow the link to the stock details page.
            crumb = response.css('script').re_first('user":{"crumb":"(.*?)"')#.decode('unicode_escape')            
            url = ("https://query1.finance.yahoo.com/v7/finance/download/" +stock +
               "?period1=-2208988800&period2=" + str(int(time.time())) + "&interval=1d&events=history&" +
               "crumb={}".format(crumb))
            yield scrapy.Request(url, meta={'stock':stock}, headers=self.myHeaders, callback=self.parse_csv)
  

    def parse_csv(self, response):
        data = response.body.decode('utf-8')
        loadData(response.meta['stock'],data)

