from scrapy_impersonate import ImpersonateDownloadHandler
from scrapy.http.request import Request
from scrapy.spiders import Spider
from twisted.internet.defer import Deferred


class PriceraImpersonateDownloadHandler(ImpersonateDownloadHandler):
    def download_request(self, request: Request, spider: Spider) -> Deferred:
        if not request.meta.get("impersonate"):
            request.meta["impersonate"] = "chrome136"

        return super().download_request(request, spider)
