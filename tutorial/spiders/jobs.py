# -*- coding: utf-8 -*-
import scrapy
from tutorial.items import TutorialItem

class JobsSpider(scrapy.Spider):
    name = 'jobs'
    allowed_domains = ['www.python.org/jobs']
    start_urls = ['http://www.python.org/jobs/']

    def parse(self, response):
        item =  TutorialItem()
        item["job_title"] = response.css("span.listing-company-name a::text").getall()
        item["location"] = response.css("span.listing-location a::text").getall()
        yield item

        
