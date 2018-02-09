# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.pipelines.images import ImagesPipeline


class RecetteScrapPipeline(object):
    def process_item(self, item, spider):
        item.pop('image_urls', None)
        item.pop('images', None)
        return item


class MyImagesPipeline(ImagesPipeline):

    def image_key(self, url):
        image_guid = url.split('/')[-1]
        name = image_guid.split('_')[0] + '.' + image_guid.split('.')[-1]
        return 'full/%s' % (name)