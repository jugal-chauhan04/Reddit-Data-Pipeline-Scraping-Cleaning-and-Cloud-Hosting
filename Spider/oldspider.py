import scrapy
import time
import schedule


class OldspiderSpider(scrapy.Spider):
    name = "oldspider"
    allowed_domains = ["old.reddit.com"]
    start_urls = ["https://old.reddit.com"]

    def parse(self, response):
        title = response.css('a.title::text').getall()
        time = response.css('time.live-timestamp::text').extract()
        subreddit = response.css('.subreddit::text').extract()
        upvotes = response.css('.score::text').extract()

        for item in zip(title, subreddit, upvotes, time):
            items = {
                'title': item[0],
                'subreddit': item[1],
                'upvote': item[2],
                'time': item[3]
            }

            yield items

        next_page = response.css('span.next-button a::attr(href)').get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    



