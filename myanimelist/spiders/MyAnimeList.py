# -*- coding: utf-8 -*-
import scrapy
import numpy as np
from myanimelist.items import AnimeItem, ReviewItem, ProfileItem
from pymongo import MongoClient

# https://myanimelist.net/topanime.php?limit=<limit>
# 
# scrapy runspider myanimelist/spiders/MyAnimeList.py 
# -a start_limit=<start_limit> 
# -a end_limit=<end_limit> 
# -s MONGODB_URL=<mongo_uri>
#
client = MongoClient()
gaydb = client.gaynime
gaynimes = gaydb.gaynimes

maldb = client['myanimelist']
reviews = maldb.reviews

class MyAnimeListSpider(scrapy.Spider):
    name = 'MyAnimeList'
    allowed_domains = ['myanimelist.net']

    def start_requests(self):
      for gaynime in gaynimes.find():
        yield scrapy.Request(f'https://myanimelist.net/{gaynime["type"]}/{gaynime["idMal"]}')

    # https://myanimelist.net/anime/5114
    def parse(self, response):
      attr   = {}
      attr['link']   = scrapy.linkextractors.LinkExtractor(restrict_text="Details").extract_links(response)[0].url
      attr['uid']    = self._extract_anime_uid(response.url)
      attr['title']  = response.css("h1 strong ::text").extract_first()
      attr['synopsis'] = " ".join(response.css("p[itemprop='description'] ::text").extract())
      attr['score']  = response.css("div.score ::Text").extract_first()
      attr['ranked'] = response.css("span.ranked strong ::Text").extract_first()
      attr['popularity'] = response.css("span.popularity strong ::Text").extract_first()
      attr['members'] = response.css("span.members strong ::Text").extract_first()
      attr['genre']   = response.css("div span[itemprop='genre'] ::text").extract()
      attr['img_url'] = response.css("a[href*=pics] img::attr(src)").extract_first()

      # status = response.css("div.js-scrollfix-bottom div.spaceit ::text").extract()
      # status = [i.replace("\n", "").strip() for i in status]

      # attr['episodes'] = status[2]
      # attr['aired']   = status[5]

      # / Anime
      yield AnimeItem(**attr)

      # /reviews
      yield response.follow("{}/{}".format(attr['link'], "reviews?spoiler=on&p=1"), self.parse_list_review)


    # https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood/reviews
    def parse_list_review(self, response):
      p = response.url.split("p=")[1]

      review_links = scrapy.linkextractors.LinkExtractor(restrict_css="a[data-ga-click-type=review-permalink]").extract_links(response)
      for review_link in review_links:
        link_uid = review_link.url.split("id=")[1]
        if not reviews.find_one({'uid': link_uid}):
          yield response.follow(review_link.url, self.parse_review)

      # None, First Page and not last page
      next_page = response.css("div.ml4.mb8 a::attr(href)").extract()
      print(next_page)
      if next_page is not None and len(review_links) > 0 and len(next_page) > 0 and (p == '1' or len(next_page) > 1):
        next_page = next_page[0] if p == '1' else next_page[1]
        yield response.follow(next_page, self.parse_list_review)
    
    # https://myanimelist.net/reviews.php?id=<uid>
    def parse_review(self, response):
      attr   = {}
      attr['link']      = response.url
      attr['uid']       = response.url.split("id=")[1]
      attr['anime_uid'] = self._extract_anime_uid(response.css("a.title.ga-click ::attr(href)").extract_first())

      # url_profile       = response.css("td a[href*=profile] ::attr(href)").extract_first()
      # attr['profile']   = url_profile.split("/")[-1]
      attr['text']      = " ".join(response.css("div.text ::text").extract()) 

      # scores            =  np.array(response.css("div.text td ::text").extract())
      # scores = dict(zip(scores[[i for i in range(12) if (i%2) == 0]], 
      #                     scores[[i for i in range(12) if (i%2) == 1]] ))
      # attr['scores']    = scores
      # attr['score']     = scores['Overall']

      # /review
      yield ReviewItem(**attr)

    # https://myanimelist.net/profile/<uid>
    def parse_profile(self, response):
      attr   = {}
      attr['link']     = response.url
      attr['profile']  = response.url.split("/")[-1]

      url_favorites = response.css("ul.favorites-list.anime li div.data a ::attr(href)").extract()
      attr['favorites'] = [self._extract_anime_uid(url) for url in url_favorites]

      user_status   = response.css("div.user-profile ul.user-status li.clearfix ::text").extract()
      user_status   = self._list2dict(user_status)

      attr['gender']   = user_status['Gender'] if 'Gender' in user_status else ''
      attr['birthday'] = user_status['Birthday'] if 'Birthday' in user_status else ''

      yield ProfileItem(**attr)

    def _extract_anime_uid(self, url):
      return url.split("/")[4]

    def _list2dict(self, attrs):
      attrs = np.array(attrs)
      attrs = dict(zip(attrs[[i for i in range(len(attrs)) if (i%2) == 0]], 
                          attrs[[i for i in range(len(attrs)) if (i%2) == 1]] ))
      return attrs
