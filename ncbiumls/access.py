import requests
from lxml import html
from ncbiumls.authorization import authorization
from time import sleep
import aiohttp
import asyncio

class UMLS(authorization):
    def __init__(self, api_key='e9990d5d-ff93-40f1-9f60-29ce7cf2950c'):
        super().__init__(str(api_key))

    async def fetch(self, session, url, params, pageNumber, term):
        """

        :param session:
        :param url:
        :param params:
        :param pageNumber:
        :param term:
        :return:
        """
        try:
            params['ticket'] = await self.asyncST()
            params['pageNumber'] = pageNumber
            params['string'] = term
            async with session.get(url, params=params) as resp:
                asyncio.wait(0.3)
                return await resp.json()
        except Exception as e:
            print(e)

    async def taskDispatcher(self, session, url, params, term, pageNumber):
        """

        :param session:
        :param url:
        :param params:
        :param term:
        :param pageNumber:
        :return:
        """
        #TODO: Think of how is it possible to organize long processing runs in parallel
        #TODO: if amount of defenitions is too overwhelming
        response = await self.fetch(session, url, params, pageNumber, term)
        data = []
        try:
            if response['result'] == {'results': [{'ui': 'NONE', 'name': 'NO RESULTS'}],
                                                    'classType': 'searchResults'}:
                return data
            else:
                for item in response['result']['results']:
                    data.append(item)
                if len(data) < params['pageSize']:
                    return data
                else:
                    pageNumber += 1
                    data += await self.taskDispatcher(session, url, params, term, pageNumber)
                    return data
        except Exception as e:
            print(e)
            return data

    async def askTermsCoro(self, terms, params, limit):
        """

        :param terms:
        :param params:
        :param limit:
        :return:
        """
        url = 'https://uts-ws.nlm.nih.gov/rest/search/current'
        tasks = []
        conn = aiohttp.TCPConnector(limit=limit)
        async with aiohttp.ClientSession(connector=conn) as session:
            for term in terms:
                task = asyncio.ensure_future(self.taskDispatcher(session, url, params, term, 1))
                tasks.append(task)
                asyncio.wait(0.3)
            responses = await asyncio.gather(*tasks)
            return responses

    def askTerms(self, terms, inputType='atom', includeObsolete='false', includeSuppressible='false',
                returnIdType='concept', searchType='words', pageSize=1000, limit=10):
        """
        Description: retrives all records associted with the terms, returns list of lists containing dicts,
                     each dict have keywords name,uri,ui,rootSource.


        :param terms:               A human readable term, such as ‘gestatational diabetes’, or a code from a source
                                    vocabulary, such as 11687002 from SNOMEDCT_US.

        :param inputType:           Specifies the data type you are using as your search parameter.
                                    Valid values:  ‘atom’, ‘code’,‘sourceConcept’,‘sourceDescriptor’,‘sourceUi’,‘tty’
                                    Default value: ‘atom’
                                    Usage Note:     Use ‘sourceUi’ if you aren’t sure if the identifier you’re providing
                                                    is a code, source concept, or source descriptor. Using ‘tty’ is for
                                                    advanced use cases and will extract codes from a specified
                                                    vocabulary according to term type.

        :param includeObsolete:     Return content that is a result of matches on obsolete terms.
                                    Valid values:   true or false
                                    Default value: ‘false'

        :param includeSuppressible: Return content that is a result of matches on suppressible terms.
                                    Valid values:   true or false
                                    Default value: ‘false'

        :param returnIdType:        Specifies the type of identifier you wish to retrieve.
                                    Valid values:   true or false
                                    Default value: ‘false'
                                    Usage Note:     Use ‘code’,‘sourceConcept’, ‘sourceDescriptor’, or ‘sourceUi’ if you
                                                    prefer source-asserted identifiers rather than CUIs in your search
                                                    results.

        :param searchType:          Type of search you wish to use.
                                    Valid values:  ‘exact’,‘words’,‘leftTruncation’, ‘rightTruncation’,‘approximate’,
                                                   ‘normalizedString’
                                    Default value: ‘words’
                                    Usage Note:     Use ‘exact’ when using inputType = ‘code’, ‘sourceConcept’,
                                                   ‘sourceDescriptor’, or ‘sourceUi’.

        :param pageNumber:          Whole number that specifies which page of results to fetch
                                    Valid values:   1,2,3, etc
                                    Default value:  1

        :param pageSize:            Whole number that specifies the number of results to include per page.
                                    Valid values:   1,2,3, etc
                                    Default value:  25

        :return:
        """

        params = {'ticket': '',
                  'string': '',
                  'inputType': inputType,
                  'includeObsolete': includeObsolete,
                  'includeSuppressible': includeSuppressible,
                  'returnIdType': returnIdType,
                  'searchType': searchType,
                  'pageNumber': 1,
                  'pageSize': pageSize}
        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(self.askTermsCoro(terms, params, limit))
        return data

    def askTerm(self, term, inputType='atom', includeObsolete='false', includeSuppressible='false',
                returnIdType='concept', searchType='words', pageNumber=1, pageSize=25):

        #   TODO: Add option like dump, if enabled - save data, current page, page size
        #   TODO: Write some messages on succesfull access, like pageNumber etc.

        """
        Description: retrives all records associted with the term, returns list of dicts,
                     each dict have keywords name,uri,ui,rootSource.


        :param term:                A human readable term, such as ‘gestatational diabetes’, or a code from a source
                                    vocabulary, such as 11687002 from SNOMEDCT_US.

        :param inputType:           Specifies the data type you are using as your search parameter.
                                    Valid values:  ‘atom’, ‘code’,‘sourceConcept’,‘sourceDescriptor’,‘sourceUi’,‘tty’
                                    Default value: ‘atom’
                                    Usage Note:     Use ‘sourceUi’ if you aren’t sure if the identifier you’re providing
                                                    is a code, source concept, or source descriptor. Using ‘tty’ is for
                                                    advanced use cases and will extract codes from a specified
                                                    vocabulary according to term type.

        :param includeObsolete:     Return content that is a result of matches on obsolete terms.
                                    Valid values:   true or false
                                    Default value: ‘false'

        :param includeSuppressible: Return content that is a result of matches on suppressible terms.
                                    Valid values:   true or false
                                    Default value: ‘false'

        :param returnIdType:        Specifies the type of identifier you wish to retrieve.
                                    Valid values:   true or false
                                    Default value: ‘false'
                                    Usage Note:     Use ‘code’,‘sourceConcept’, ‘sourceDescriptor’, or ‘sourceUi’ if you
                                                    prefer source-asserted identifiers rather than CUIs in your search
                                                    results.

        :param searchType:          Type of search you wish to use.
                                    Valid values:  ‘exact’,‘words’,‘leftTruncation’, ‘rightTruncation’,‘approximate’,
                                                   ‘normalizedString’
                                    Default value: ‘words’
                                    Usage Note:     Use ‘exact’ when using inputType = ‘code’, ‘sourceConcept’,
                                                   ‘sourceDescriptor’, or ‘sourceUi’.

        :param pageNumber:          Whole number that specifies which page of results to fetch
                                    Valid values:   1,2,3, etc
                                    Default value:  1

        :param pageSize:            Whole number that specifies the number of results to include per page.
                                    Valid values:   1,2,3, etc
                                    Default value:  25

        :return:
        """
        # main url and parameters
        url = 'https://uts-ws.nlm.nih.gov/rest/search/current'
        params = {'ticket': '',
                  'string': str(term),
                  'inputType': inputType,
                  'includeObsolete': includeObsolete,
                  'includeSuppressible': includeSuppressible,
                  'returnIdType': returnIdType,
                  'searchType': searchType,
                  'pageNumber': pageNumber,
                  'pageSize': pageSize}

        #varible to store received data
        data = []

        #in case specific page is not provided - grab all results starting from page 1
        if pageNumber == 1:

            #function is guaranteed to return results so ask cycle will roll infinitely
            while True:

                #get a service ticket
                params['ticket'] = self.getST()

                #access page
                get = requests.get(url=url, params=params)
                try:

                    #if all the results obtained return gathered data
                    if get.json()['result'] == {'results': [{'ui': 'NONE', 'name': 'NO RESULTS'}],
                                                  'classType': 'searchResults'}:
                        return data

                    #else grab some more data
                    else:
                        for item in get.json()['result']['results']:
                            data.append(item)
                        params['pageNumber'] += 1

                #if server returned error - try to print that error, wait and retry request
                except Exception as exp:
                    response = html.fromstring(data.text)
                    print(response.xpath('.//p/b[contains(text(),"description")]/following::u')[0].text,
                          ', current page number = ' + str(pageNumber),
                          ', current page size = ' + str(pageSize))

                #to not overlode server pause a bit
                sleep(0.5)

        #else access specific page
        else:
            params['ticket'] = self.getST()
            get = requests.get(url=url, params=params)
            try:
                return get.json()['result']['results']
            except Exception as exp:
                response = html.fromstring(data.text)
                print(response.xpath('.//p/b[contains(text(),"description")]/following::u')[0].text,
                      ', current page number = ' + str(pageNumber),
                      ', current page size = ' + str(pageSize))

class CUI(authorization):
    def __init__(self, api_key='e9990d5d-ff93-40f1-9f60-29ce7cf2950c'):
        super().__init__(api_key=api_key)

    async def fetch(self, session, url, params, pageNumber, term):
        try:
            params['pageNumber'] = pageNumber
            params['ticket'] = await self.asyncST()
            accessUrl = url+term
            async with session.get(accessUrl, params=params) as resp:
                asyncio.wait(0.3)
                return await resp.json()
        except Exception as e:
            print(e, 'error on fetch')
            return 'error'

    async def taskDispatcher(self, session, url, params, term, pageNumber):
        #TODO: Think of how is it possible to organize long processing runs in parallel
        #TODO: if amount of defenitions is too overwhelming

        try:
            response = await self.fetch(session, url, params, pageNumber, term)
            return response['result']

        except Exception as e:
            print(e, 'error on taskDispatcher')
            return 'error'

    async def askTermsCoro(self, terms, params, url, limit):
        tasks = []
        conn = aiohttp.TCPConnector(limit=limit)
        async with aiohttp.ClientSession(connector=conn) as session:
            for term in terms:
                task = asyncio.ensure_future(self.taskDispatcher(session, url, params, term, 1))
                tasks.append(task)
                asyncio.wait(0.3)
            responses = await asyncio.gather(*tasks)
            return responses

    def askCUIs(self, cuis, limit=30):
        """
        :param cui: The Concept Unique Identifier for a Metathesaurus concept
        :return: Returns dictionary processed from JSON, example:
                {
                    "pageSize": 25,
                    "pageNumber": 1,
                    "pageCount": 1,
                    "result": {
                        "classType": "Concept",
                        "ui": "C0009044",
                        "suppressible": false,
                        "dateAdded": "09-30-1990",
                        "majorRevisionDate": "08-18-2015",
                        "status": "R",
                        "semanticTypes": [
                            {
                                "name": "Injury or Poisoning",
                                "uri": "https://uts-ws.nlm.nih.gov/rest/semantic-network/2015AB/TUI/T037"
                            }
                        ],
                        "atomCount": 63,
                        "attributeCount": 0,
                        "cvMemberCount": 0,
                        "atoms": "https://uts-ws.nlm.nih.gov/rest/content/2015AB/CUI/C0009044/atoms",
                        "definitions": "NONE",
                        "relations": "https://uts-ws.nlm.nih.gov/rest/content/2015AB/CUI/C0009044/relations",
                        "defaultPreferredAtom": "https://uts-ws.nlm.nih.gov/rest/content/
                                                        2015AB/CUI/C0009044/atoms/preferred",
                        "relationCount": 5,
                        "name": "Closed fracture carpal bone"
                    }
                }
        """
        url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/'
        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(self.askTermsCoro(cuis, params={}, url=url, limit=limit))
        return data


    def CUIGeneral(self, cui):
        """
        :param cui: The Concept Unique Identifier for a Metathesaurus concept
        :return: Returns dictionary processed from JSON, example:
                {
                    "pageSize": 25,
                    "pageNumber": 1,
                    "pageCount": 1,
                    "result": {
                        "classType": "Concept",
                        "ui": "C0009044",
                        "suppressible": false,
                        "dateAdded": "09-30-1990",
                        "majorRevisionDate": "08-18-2015",
                        "status": "R",
                        "semanticTypes": [
                            {
                                "name": "Injury or Poisoning",
                                "uri": "https://uts-ws.nlm.nih.gov/rest/semantic-network/2015AB/TUI/T037"
                            }
                        ],
                        "atomCount": 63,
                        "attributeCount": 0,
                        "cvMemberCount": 0,
                        "atoms": "https://uts-ws.nlm.nih.gov/rest/content/2015AB/CUI/C0009044/atoms",
                        "definitions": "NONE",
                        "relations": "https://uts-ws.nlm.nih.gov/rest/content/2015AB/CUI/C0009044/relations",
                        "defaultPreferredAtom": "https://uts-ws.nlm.nih.gov/rest/content/
                                                        2015AB/CUI/C0009044/atoms/preferred",
                        "relationCount": 5,
                        "name": "Closed fracture carpal bone"
                    }
                }
        """
        stTicket = self.getST()
        url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + str(cui)
        params = {'ticket': stTicket}
        data = requests.get(url=url, params=params)
        try:
            return data.json()
        except Exception as exp:
            response = html.fromstring(data.text)
            return response.xpath('.//p/b[text()="message"]/u')

    def CUISemanticTypes(self, cui):
        """
        :param cui: The Concept Unique Identifier for a Metathesaurus concept
        :return: Returns dictionary processed from JSON, example:
                {
                    "pageSize": 25,
                    "pageNumber": 1,
                    "pageCount": 1,
                    "result": {
                        "classType": "Concept",
                        "ui": "C0009044",
                        "suppressible": false,
                        "dateAdded": "09-30-1990",
                        "majorRevisionDate": "08-18-2015",
                        "status": "R",
                        "semanticTypes": [
                            {
                                "name": "Injury or Poisoning",
                                "uri": "https://uts-ws.nlm.nih.gov/rest/semantic-network/2015AB/TUI/T037"
                            }
                        ],
                        "atomCount": 63,
                        "attributeCount": 0,
                        "cvMemberCount": 0,
                        "atoms": "https://uts-ws.nlm.nih.gov/rest/content/2015AB/CUI/C0009044/atoms",
                        "definitions": "NONE",
                        "relations": "https://uts-ws.nlm.nih.gov/rest/content/2015AB/CUI/C0009044/relations",
                        "defaultPreferredAtom": "https://uts-ws.nlm.nih.gov/rest/content/
                                                        2015AB/CUI/C0009044/atoms/preferred",
                        "relationCount": 5,
                        "name": "Closed fracture carpal bone"
                    }
                }
        """
        stTicket = self.getST()
        url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + str(cui)
        params = {'ticket': stTicket}
        data = requests.get(url=url, params=params)
        try:
            return data.json()['result']['semanticTypes']

        except Exception as exp:
            response = html.fromstring(data.text)
            return response.xpath('.//p/b[text()="message"]/u')

    def CUIAtoms(self, cui, pageNumber=1, pageSize=25):
        stTicket = self.getST()
        url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + str(cui) + '/atoms'
        params = {'ticket': stTicket,
                  'pageNumber': pageNumber,
                  'pageSize': pageSize}
        data = requests.get(url=url, params=params)
        pass

    def CUIDefenitions(self, cui, pageNumber=1, pageSize=25):
        stTicket = self.getST()
        url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + str(cui) + '/definitions'
        params = {'ticket': stTicket,
                  'pageNumber': pageNumber,
                  'pageSize': pageSize}
        data = requests.get(url=url, params=params)
        pass

    def CUIRelations(self, cui, pageNumber=1, pageSize=25):
        stTicket = self.getST()
        url = 'https://uts-ws.nlm.nih.gov/rest/content/current/CUI/' + str(cui) + '/relations'
        params = {'ticket': stTicket,
                  'pageNumber': pageNumber,
                  'pageSize': pageSize}
        data = requests.get(url=url, params=params)
        pass


if __name__ == '__main__':
    with open('output.txt') as read:
        exec(read.read())
    print(len(ChemIds))
    print(ChemIds[:2])
    data = UMLS()
    results = data.askTerms(ChemIds[:10], inputType='code', searchType='exact')
    for item in results:
        print(len(item), item)
