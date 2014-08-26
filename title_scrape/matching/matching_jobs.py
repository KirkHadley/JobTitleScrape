from fuzzywuzzy import fuzz
import pandas as pd
import numpy as np
from copy import deepcopy
import multiprocessing as mp
from time import time
from datetime import datetime as dt
jobsdf = pd.read_csv('../indeed_scrape/all_jobs.csv')
del jobsdf[jobsdf.columns[0]]
jobsdf.columns = ['industry', 'title', 'search_term', 'city', 'state', 'country', 'pretty_location', 'short_desc', 'start']
del jobsdf['country']
#jobsdf1 = jobsdf2
#space before numerals is intentional


def clean_titles(jobsdf, rules):
    ''' 
function to replace weird strings/chars in job titles. takes a dataframe of jobs- preferably the one created by the indeed_scrape script as well as a list of rules. list of rules must be list of 2-item lists where first item is character(s) to be replaced and second item is the character(s) used in the replacement.  

    '''
    for i in range(len(jobsdf['title'])):
        if i == 0 or i  % 1000 == 0:
            print 'cleaned ', i, ' titles'
        for x in range(len(rules)):
            jobsdf['title'].ix[i] = jobsdf['title'].ix[i].replace(x[0], x[1])
    return jobsdf

def clean_frame(jobsdf, rules=None):
    '''
    get counts, remove duplicates, recalibrate axis. counts column + freq from check_all will equal total number of occurences for a particular title. accepts same arguments as clean_titles
    '''
    jobsdf['counts']  = jobsdf.groupby(['title'])['industry'].transform('count')    
    jobsdf.drop_duplicates('title', inplace=True)
    jobsdf.index = range(len(jobsdf))
    if rules:
        jobsdf = clean_titles(jobsdf, rules)
    return jobsdf

def check_term(pos, x, thresh,  verbose = False):
    ''' 
    function to find highly similar matches to a single job title. uses token_set_ratio from the fuzzywuzzy package to tokenize and regularize strings before computing their edit distance. the thresh argument (int) is the threshold for determing similarity between two terms. returns a dict w/ the keys ['title', 'freq', 'similar_terms'].
    '''
    df = x
    test_dict = dict()
    similar_terms = [ ]
    for i in range(pos, len(df['title'])):            
        if verbose:
            if i % 1000 == 0:
                print i
        if pd.notnull(df['title'].ix[i]):
            if pos != i:
                a = fuzz.token_set_ratio(df['title'].ix[pos], df['title'].ix[i])
                if a > thresh:
                    similar_terms.append([df['title'].ix[i], a, df['industry'].ix[i], i])
                    df['title'].ix[i] = np.nan
        else:
            pass
    if len(similar_terms) > 0:
        test_dict['title'] = df['title'].ix[pos]       
        test_dict['similar_titles'] = similar_terms
        test_dict['freq'] = df['counts'].ix[pos] + len(similar_terms)
        test_dict['city'] = df['city'].ix[pos]
        test_dict['state'] = df['state'].ix[pos]
    else:
        test_dict['title'] = df['title'].ix[pos]
        test_dict['similar_titles'] = 'No Similar Titles'
        test_dict['freq'] = df['counts'].ix[pos]
        test_dict['city'] = df['city'].ix[pos]
        test_dict['state'] = df['state'].ix[pos]
    ts = time()
    st = dt.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    print 'finished',  df['title'].ix[pos], pos, st
    return test_dict

def check_all(thresh, dataframe):
    '''
    implements check_term over an entire jobs dataframe. returns a list of dicts of the same structure as created by check_term. each unique job title will have one entry in the list
    *** this was way too slow, kept in code for clarity's sake. reimplemented this functionality in check_term_parallel
    '''
    l = [ ]
    num = 0
    while num < len(dataframe):
        l.append(check_term(num, thresh, dataframe))    
        num = num + 1
        print num
    return l
def check_all_parallel(jobsdf, thresh, cores=None):
    ''' 
    this is the version of check_all actually being used
    supply same arguments as first check_all, as well as the number of cores you'd like to use
    if cores is not specified, the function will use all but two of your machine's cores
    '''
    if cores:
        processes = cores
    else:
        processes = mp.cpu_count() - 2
    pool = mp.Pool(processes = processes)
    results = [pool.apply_async(check_term, args = (pos, jobsdf, thresh) for pos in range(len(jobsdf))] 
    results = [p.get() for p in results]
    return results
 
def create_df(jobframe):
    '''
    creates a dataframe from the list returned by check_all
    '''

    l = [ ]
    for i in range(len(jobframe)):
        if pd.notnull(jobframe[i]['title']):
            inds = [ ]
            for x in range(len(jobframe[i]['similar_titles'])):
                inds.append(jobframe[i]['similar_titles'][x][0])
            #inds = set(inds)  
            similar  = ', '.join(inds)
            freq = jobframe[i]['freq']
            title = jobframe[i]['title']
            city = jobframe[i]['city']
            state = jobframe[i]['state']
            try:
                industry = jobframe[i]['similar_titles'][0][2]
            except:
                industry = 'Miscellaneous, Unknown, or Multiple Industries'
            l.append([similar, industry, title, freq, city, state])
        else:
            pass
    df = pd.DataFrame(l)
    for i in range(len(df[0])):
        # bug somewhere in check_terms, not worth re-running
        if df[0].ix[i] == 'N, o,  , S, i, m, i, l, a, r,  , T, i, t, l, e, s':
            df[0].ix[i] = 'No Similar Titles'
        else:
            pass
    return df

def matcher(jobsdf, thresh, rules=None, cores=None):
    ''' 
    convenience function wrapping clean_frame, check_all, and create_df. returns a dataframe
    '''
    jobs = clean_frame(jobsdf, rules)
    jobs = check_all_parallel(jobs, thresh, cores)
    jobs = create_df(jobs)
    return jobs


