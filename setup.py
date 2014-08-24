from setuptools import setup, find_packages
setup(
    name = 'title_scrape', 
    packages = find_packages(), 
    description = 'scrape indeed for job titles', 
    author = 'Frederick Hadley', 
    install_requires = ['pandas', 
            'bs4', 
            'urllib2', 
            'requests']
)
 
