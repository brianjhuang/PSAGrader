from scrape_auction_prices import PsaAuctionPrices
#https://github.com/ChrisMuir/psa-scrape/blob/master/auction_prices_realized/scrape_auction_prices.py
#import in the code that scrapes the PSA site
import os
import sys
import time
import math
import pandas as pd
import requests
import shutil # to save it locally
import concurrent.futures

class generateData:
    '''
    Get the text file that contains all the links
    to all the tables on a page.
    '''
    links = []
    data = []
    output_csv = ""
    def __init__(self, output_csv):
        '''
        output_csv (string) - the name of our output csv file.
        '''
        self.output_csv = output_csv

    def getLinks(self, filepath):
        '''
        Params:
        filepath (string) - the name of the text file

        Returns:
        The cleaned text file ready to scrape
        '''
        base_link = 'https://www.psacard.com/'
        with open(filepath, 'r') as file:
            for f in file.readlines():
                self.links.append((base_link + f.strip('\u200b').strip(' ')[1:]).strip('\n'))
            #strip the file of the lines that the javascript file generates
        with open(filepath.strip('.txt') + ('_links.txt'), 'w') as file:
            for link in self.links:
                file.write(link + '\n')
            #re load the cleaned data
        return self.links
    
    def scrapeCSV(self, links):
        '''
        Scrape our CSVs.
        '''
        if not os.path.exists("data"):
            os.makedirs("data")
        if type(links) == list:
            urls = links
        else:
            with open(links, 'r') as file:
                urls = file.readlines()
        for url in urls:
            # Initialize class and execute web scraping
            pap = PsaAuctionPrices(url)
            pap.scrape()

    def createData(self, filepath):
        '''
        Take in the location of all the CSVs and return
        a dataframe with all the CSVs concatenated.
        '''
        csv_files = os.listdir(filepath)
        for csv in csv_files:
            if filepath + csv == 'psa-scrape-master/auction_prices_realized/data/.ipynb_checkpoints':
                #if you are running this in a jupyter notebook
                continue
            self.data.append(pd.read_csv(filepath + csv))
        df = pd.concat(self.data)
        df.to_csv(self.output_csv, index = False)
        return df

    def scrapeImages(self, df, sets):
        '''
        This returns all the scraped images to a folder.
        It takes in our dataframe from PSAGrade, assuming
        all column names are correct and scrapes the images.

        df - the dataframe
        set - the set number (or in this case the batch of data)
        base set was our first batch so it has no set number, but
        any other set will be labelled with an id
        '''
        if not os.path.exists("Images"):
            os.makedirs("Images")
        start = time.time()
        success = []
        fail = []
        for i in range(len(df)):
            r = requests.get(df['img_url'][i], stream = True)
            filename = 'set-' + str(sets) +'-' + str(i) + '.jpg'
            if r.status_code == 200:
                # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
                r.raw.decode_content = True
                out_path = 'Images/' + filename

                # Open a local file with wb ( write binary ) permission.
                with open(out_path,'wb') as f:
                    shutil.copyfileobj(r.raw, f)
                print('Image sucessfully Downloaded: ',filename)
                print(time.time() - start)
                success.append(i)
            else:
                print('Image Couldn\'t be retreived')
                print(time.time() - start)
                fail.append(i)
        return success, fail

    def threadedScrape(self, df, sets):
        '''
        Same as the scraper from before, however use with caution.
        Once this threaded scraper is started, it cannot be stopped.
        '''
        def scrapeImage(index, img):
            r = requests.get(img, stream = True)
            filename = 'set' + str(sets) + str(i) + '.jpg'
            if r.status_code == 200:
                # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
                r.raw.decode_content = True
                out_path = 'Images/' + filename

                # Open a local file with wb ( write binary ) permission.
                with open(out_path,'wb') as f:
                    shutil.copyfileobj(r.raw, f)
                print('Image sucessfully Downloaded: ',filename)
            else:
                print('Image Couldn\'t be retreived')

        image_urls = list(zip(df.index, df['img_url']))
        t1 = time.perf_counter()


        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(lambda p: scrapeImage(*p), image_urls)


        t2 = time.perf_counter()
