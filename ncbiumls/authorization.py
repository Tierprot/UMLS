import aiohttp
import asyncio
#import requests
from lxml import html
import datetime

class authorization:
    """
    To note: TGT (ticket-granting ticket) is valid only for 8 hours, after that one needs to
             receive new TGT
    """
    tgtTimeLimit = 8

    def __init__(self, api_key='e9990d5d-ff93-40f1-9f60-29ce7cf2950c'):
        self.key = api_key
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.authorize(key=self.key))

    async def authorize(self, key):
        """
        Authorizes user by instantiating two auriozation relating variables tgt and tgtExpirationTime.
        tgt - stores TGT key which is requiered to obtain ST
        tgtExpirationTime - stores time util when TGT key is applicable

        :param key: API key from the UTS ‘My Profile’ area after signing in (https://uts.nlm.nih.gov/home.html).
                    An API key remains active as long as the associated UTS account is active.

        :return:    n/s
        """

        url = "https://utslogin.nlm.nih.gov/cas/v1/api-key"
        params = {'apikey': key}
        headers = {"Content-type": "application/x-www-form-urlencoded",
                   "Accept": "text/plain",
                   "User-Agent": "Py"}
        async with aiohttp.ClientSession() as session:
            req = await session.post(url=url, data=params, headers=headers)
        text = await req.text()
        self.tgt = html.fromstring(text).find('.//form').action
        self.tgtExpirationTime = datetime.datetime.now() + \
                                 datetime.timedelta(hours=authorization.tgtTimeLimit)

    async def asyncST(self):
        """
        Gets a Service Ticket. A Service Ticket expires after one use or five minutes from the time of generation.

        :return: Service Ticket.
        """

        if datetime.datetime.now() >= self.tgtExpirationTime:
            self.tgt = self.authorize(key=self.key)
        params = {'service': 'http://umlsks.nlm.nih.gov'}
        async with aiohttp.ClientSession() as session:
            data = await session.post(self.tgt, data=params)
        return await data.text()

    def getST(self):
        loop = asyncio.get_event_loop()
        ticket = loop.run_until_complete(self.asyncST())
        return ticket

if __name__ == '__main__':
    data = authorization()
    print(data.getST())
    print(data.tgt.split('/')[-1])
