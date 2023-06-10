# BullsEye
Find the right image for any valid webpage
The Lib is designed to get the correct image for any valid url
Simply run quickstart to test it out.

Caveats:
1 Currently to avoid retry crashes, ensure that the file 'middleware scrapflu.scrapy" process exception is commented out as sucj
    
    ```
    def process_exception(self, request, exception:Union[str, Exception], spider:ScrapflySpider):
        delay = 1

        """if isinstance(exception, ResponseNeverReceived):
            return spider.retry(request, exception, delay)

        if isinstance(exception, ScrapflyError):
            if exception.is_retryable:
                if isinstance(exception, HttpError) and exception.response is not None:
                    if 'retry-after' in exception.response.headers:
                        delay = int(exception.response.headers['retry-after'])

                return spider.retry(request, exception, delay)

            if spider.settings.get('SCRAPFLY_CUSTOM_RETRY_CODE', False) and exception.code in spider.settings.get('SCRAPFLY_CUSTOM_RETRY_CODE'):
                return spider.retry(request, exception, delay)"""

        raise exception
        ```
        
    2 There is an open ticket on "sporadic 504 " errors with the scrapfly devs
