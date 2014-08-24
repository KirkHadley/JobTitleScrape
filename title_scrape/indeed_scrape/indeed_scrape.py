import pandas as pd
from bs4 import BeautifulSoup
import requests
import json


base_url = 'http://api.indeed.com/ads/apisearch?publisher=1450252626268926&format=json&v=2&limit=25&q='

def url_creation(industry_df):
    urls = [ ]
    for i in industry_df['term']:
        urls.append(base_url + i + '&start=' + str(0))
        #urls.append(base_url + i + '&start=' + str(25))
        #urls.append(base_url + i + '&start=' + str(50))
        #urls.append(base_url + i + '&start=' + str(75))
        
    return urls
def get_all(urls):
    content = [ ]
    pulled = 0
    for i in urls:
        req = requests.request('GET', i)
        print req.status_code
        content.append(req.content)
        pulled = pulled + 1
        print pulled
    for i in range(len(content)):
        content[i] = json.loads(content[i])
    return content

def reduce_info(results_list, industry_df):
    l = [ ]
    for i in range(len(results_list)): 
        for x in range(len(results_list[i]['results'])):
            short_desc  = BeautifulSoup(results_list[i]['results'][x]['snippet'])
            short_desc = short_desc.text
            title = results_list[i]['results'][x]['jobtitle']
            company = results_list[i]['results'][x]['company']
            city = results_list[i]['results'][x]['city']
            state = results_list[i]['results'][x]['state']
            country = results_list[i]['results'][x]['country']
            pretty_location = results_list[i]['results'][x]['formattedLocation']
            industry = industry_df.ix[i].values[0]
            search_term = industry_df.ix[i].values[1]
            start = results_list[i]['start']
            l.append([industry, title, search_term, city, state, country, pretty_location, short_desc, start]) 
        print 'finished: ', i, 'terms'    
    return l

def wrapper(ind_df):
    job_urls = url_creation(ind_df)
    res = get_all(job_urls)
    res = reduce_info(res, ind_df)
    res = pd.DataFrame(res)
    res.columns = ['industry', 'title', 'search_term', 'city', 'state', 'country', 'pretty_location', 'short_desc', 'start']
    if len(res['country'].unique()) < 2:
        del res['country']
    return res

#job_df = pd.read_csv( '../about_scrape/jobs.csv', index_col = 0, header = 0, names = ['industry', 'term']) 
#df = wrapper(job_df)
#df.to_csv('big_job_list.csv')

