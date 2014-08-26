from about_scrape.industries import *
from indeed_scrape.indeed_scrape import *
from matching.matching_jobs import *  
#url for list of initial industries/functions from about
industries_page = 'http://jobsearch.about.com/od/job-titles/fl/job-titles-a-z.htm'
# scrape industries from about
industries = get_industry_links(industries_page)
print 'industries downloaded'

#scrape industry-specific job titles from each industry page, to be used for spawning indeed search queries
jobs = jobs_fromlist(industries)

print "head of queries df"
print  jobs.head(10)
print "tail of queries df"
print jobs.tail(10)
jobs.to_csv('jobs1.csv', encoding='utf-8')
jobs.columns = ['industry', 'term']

#using list of search terms from about, query the indeed apifor each term. this takes a minute or two
df = scrape_indeed(jobs)
df.to_csv('big_job_list.csv')

#list of rules for cleaning jobs titles. see matching/matching.py
rules_list = [[' III', ' Sr.'], [' II', ''], [' I', ' Jr.'], ['Senior', 'Sr'], ['Junior', 'Jr'], [' 3', ' Sr.'], [' 2', ''], [' 1', ' Jr.']]
#clean job titles, find similarities
# warning: takes several hours d/t huge number of jobs (~100k)
df = matcher(df, threshold, rules_list)
df.to_csv('clean_job_list.csv')
