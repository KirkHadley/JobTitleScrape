import pandas as pd
from bs4 import BeautifulSoup
import urllib2


def correctdiv_fromurl(url):
    content = urllib2.urlopen(url).read()
    page = BeautifulSoup(content) 
    mydivs = page.find("div", { "class" : "expert-content-text" })
    return mydivs

def clean_url(x, item = 1):
    if type(x) == list:
        for i in range(len(x)):
            if type(x[i]) == list:
                x[i][item]  = 'http://jobsearch.about.com' + x[i][item]
            else:
                x[i] = 'http://jobsearch.about.com' + x[i]
        return x
    if type(x) == str:
        x = 'http://jobsearch.about.com' + x 
        return x
    else:
        print "Only accepts list or str"
            
def get_industries(mydivs):
    link_list = []
    for link in mydivs.find_all('a'):
        a = link.get('href')
        b = link.contents[0]
        if a.startswith("/od/job-title-samples/"):
            link_list.append([b,a])
        else:
            pass
    del link_list[len(link_list) - 1]
    return link_list


def get_industry_links(x):
    divs = correctdiv_fromurl(x)
    link_list = get_industries(divs)
    link_list = clean_url(link_list)
    return link_list


def jobs_frompage(industry_page):
    jobs = [ ]
    for item in industry_page.find_all('li'):
        jobs.append(item.contents[0])
    return jobs


def clean_job_titles(x):
    x = x.replace("/", "+")
    x = x.replace(" ", "+")
    x = x.replace("(", "+")
    x = x.replace(")", "")
    x = x.replace("with", "")
    x = x.replace("and", "")
    return x


def jobs_fromlist(link_list, industry = 0, link = 1):
    jobs_list = [ ] 
    for i in range(len(link_list)):
        div = correctdiv_fromurl(link_list[i][1])
        j = jobs_frompage(div)
        for a in range(len(j)):
            j[a] = clean_job_titles(j[a])
            jobs_list.append([link_list[i][0], j[a]]) 
        print i, " industries downloaded"
        print 'the ', link_list[i][0], ' industry had ',  len(j), 'jobs' 
    jobs_df = pd.DataFrame(jobs_list)
    return jobs_df







