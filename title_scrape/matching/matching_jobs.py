from fuzzywuzzy import fuzz
import pandas as pd
import numpy as np
from copy import deepcopy
import multiprocessing as mp
from time import time
from datetime import datetime as dt
import regex

    
default_cores = mp.cpu_count() - 2

def paren_ToUpper(match):
    return '(' + match.group(1).upper() + ')'

def forward(x, s):
    a = re.search(x, s)
    if a:
        if s[a.start():a.end()] == s[0:a.start()]:
            print 'string already at front'
            return s
        else:
            try:
                new_string = s[a.start():a.end()] + s[0:a.start()] + s[a.end():] 
                return new_string
            except IndexError:
                new_string = s[a.start():a.end()] + s[0:a.start()]
                return new_string            

def clean_titles(jobsdf, rules, special_char= ['/', '-', ','], forward_words = None):
    jobsdf['title'] = jobsdf['title'].replace(r'Director\s-', 'Director,', regex=True)
    jobsdf['title'] = jobsdf['title'].replace('Director Of', 'Director,')
    jobsdf['title'] = jobsdf['title'].replace('CLERK - A/P', 'CLERK  A/P')
    for r in range(len(rules)):
        if re.search(r'(?<!I)I(?!I)', rules[r][0]):
            jobsdf['title'] = jobsdf['title'].apply(lambda x: re.sub(r'(?<!\w)I(?![A-Za-z])', 'Jr.', x))
        else:       
            regex = re.compile(rules[r][0], re.I) 
            jobsdf['title'] = jobsdf['title'].apply(lambda x: regex.sub(rules[r][1],  x))
    for i in range(len(special_char)):
        if special_char[i] == "\\":    
            jobsdf['title'] = jobsdf['title'].apply(lambda x: re.split(r'(?<!A)\\(?!P)', x)[0])
        elif special_char[i] == "/":
            jobsdf['title'] = jobsdf['title'].apply(lambda x: re.split(r'(?<!A)/(?!P)', x)[0])
        elif special_char[i] == '-':
            jobsdf['title'] = jobsdf['title'].apply(lambda x: x.split('-')[0])      
        else:
            jobsdf['title'] = jobsdf['title'].apply(lambda x: x.split(special_char[i])[0]) 
    jobsdf['title'] = jobsdf['title'].apply(lambda x: re.sub(r'\s,', '', x))
    jobsdf['title'] = jobsdf['title'].apply(lambda x: x.strip()) 
    if forward_words:
        for f in range(len(forward_words)):
            jobsdf['title'] = jobsdf['title'].apply(lambda x: forward(x, forward_words[f])) 
    jobsdf['title'] = jobsdf['title'].apply(lambda x: " ".join(w.capitalize() for w in x.split()))
    jobsdf['title'] = jobsdf['title'].apply(lambda x: re.sub(r'\(([^\s]?)\)', paren_ToUpper, x))
    jobsdf['title'] = jobsdf['title'].replace("A/p", "A/P", regex=True)
    jobsdf['title'] = jobsdf['title'].replace(r'(?<!Jr|Sr)\.', '', regex=True)  
    return jobsdf 
    
def prepare_frame(jobsdf):
    '''
    get counts, remove duplicates, recalibrate axis. counts column + freq from check_all will equal total number of occurences for a particular title. only argument is the jobs data frame created with the indeed scaper
    the purpose of this function is not to return a perfectly cleaned data frame, but instead to weed out obvious errors & redudancies in order to speed the following processes
    '''
    jobsdf['counts']  = jobsdf.groupby(['title'])['industry'].transform('count')    
    jobsdf.drop_duplicates('title', inplace=True)
    jobsdf.index = range(len(jobsdf))
    return jobsdf


def check_term(pos, df, thresh,  verbose = False):
    ''' 
    function to find highly similar matches to a single job title. uses token_set_ratio from the fuzzywuzzy package to tokenize and regularize strings before computing their edit distance. the thresh argument (int) is the threshold for determing similarity between two terms. returns a dict w/ the keys ['title', 'freq', 'similar_terms'].
    '''
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
        test_dict['industry'] = df['industry'].ix[pos]
    else:
        test_dict['title'] = df['title'].ix[pos]
        test_dict['similar_titles'] = 'No Similar Titles'
        test_dict['freq'] = df['counts'].ix[pos]
        test_dict['city'] = df['city'].ix[pos]
        test_dict['state'] = df['state'].ix[pos]
        test_dict['industry'] = df['industry'].ix[pos]
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


def check_all_parallel(jobsdf, thresh, cores=default_cores):
    ''' 
    this is the version of check_all actually being used
    supply same arguments as first check_all, as well as the number of cores you'd like to use
    if cores is not specified, the function will use all but two of your machine's cores
    '''
    pool = mp.Pool(processes = cores) 
    results = [pool.apply_async(check_term, args = (pos, jobsdf, thresh)) for pos in range(len(jobsdf))] 
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
                industry = jobframe[i]['industry']
            except KeyError:
                industry = jobframe[i]['similar_titles'][0][2]
            except IndexError:
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


def matcher(jobsdf, thresh, rules, special_chars = ['/', '-', ','], cores = default_cores):
    ''' 
    convenience function wrapping clean_frame, check_all, and create_df. returns a dataframe
    '''
    jobs = clean_frameParallel(jobsdf, rules, special_chars, cores)
    jobs = check_all_parallel(jobs, thresh, cores)
    jobs = create_df(jobs)
    return jobs


