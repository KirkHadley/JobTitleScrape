from about_scrape.industries import *
from indeed_scrape.indeed_scrape import *
from matching.matching_jobs import *  

industries_page = 'http://jobsearch.about.com/od/job-titles/fl/job-titles-a-z.htm'
industries = get_industry_links(industries_page)
print 'industries downloaded'

jobs = jobs_fromlist(industries)

print "head of queries df"
print  jobs.head(10)
print "tail of queries df"
print jobs.tail(10)
jobs.to_csv('jobs1.csv', encoding='utf-8')
jobs.columns = ['industry', 'term']
#job_df = pd.read_csv( '../about_scrape/jobs.csv', index_col = 0, header = 0, names = ['industry', 'term'])
df = wrapper(jobs)
df.to_csv('big_job_list1.csv')
