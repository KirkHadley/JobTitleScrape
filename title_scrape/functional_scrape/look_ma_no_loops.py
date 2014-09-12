import pandas as pd
from simple_requests import Requests
import json
import operator

import pycurl
import os

with open(os.path.join(os.path.expanduser('~'),'JobTitleScrape/title_scrape/functional_scrape/', 'data.zip'), 'wb') as f:
	c = pycurl.Curl()
	c.setopt(c.URL, 'http://www.onetcenter.org/dl_files/2010/lay_titles.zip')
	c.setopt(c.WRITEDATA, f)
	c.perform()
	c.close()
zf  = zipfile.ZipFile("data.zip")
cols = ['soc_code', 'soc_title', 'onet_code', 'onet_title', 'job_title']
df = pd.read_csv(zf.open("lay_titles/Lay Titles.txt"), delimiter = "\t", header=None, names = cols, skiprows =1, usecols = [0,1,2,3,4])

bad_codes = ['37-1','37-2', '37-3', '45-1', '45-2', '45-3','45-4', '47-1', '47-3', '47-4','47-5','49-3', '49-9','51-3','51-4', '51-5', '51-6', '51-7', '51-8', '51-9', '55-1','55-2', '55-3', '53-3', '53-4', '53-5', '53-6', '53-7']
df['soc_code'] = df.ix[:, 'soc_code'].apply(lambda x: x[:-3])i
df = df[df['soc_code'].isin(bad_codes) == False]
df['job_title'] = df['job_title'].apply(lambda x: map(lambda x: x.capitalize(), sorted(x.lower().split(' '))))
df.sort('job_title', inplace=True)
df.drop_duplicates('job_title', inplace=True)
df.index = range(len(df))

url_base = 'http://api.indeed.com/ads/apisearch?publisher=1450252626268926&format=json&v=2&limit=25&q=title:('
urls = df['job_title'].apply(lambda x: url_base + x + ')')

requests = Requests(concurrent = 50)
y = requests.swarm(urls)
a = filter(lambda x: x['totalResults'] > 24, map(lambda x: json.loads(x.content), y))
b = map(lambda x: x['jobtitle'], reduce(operator.add, map(list,map(lambda x: x['results'], a))))
c = map(lambda x: b[x*25:(x+1)*25], range(len(a)))
data = map(list, map(None, c, map(lambda x: x['totalResults'], a), map(lambda x: x['query'][7:-1].title(), a), map(lambda x: x, range(len(a)))))
r  = map(lambda x: x, map(lambda x: x[1], data))
l  = map(lambda x: ','.join(x[:10]), map(lambda x: x[0], data))
df['total_results'] = r
df['similar_titles'] = l
df[df['total_results'] > 40].to_csv('greater_than_40_full1.csv', encoding='utf-8')
