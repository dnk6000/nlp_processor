#!C:\Work\Python\CasCrawl37\CassandraPy37\Scripts\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'VK-Scraper==2.0.3','console_scripts','vk-scraper'
__requires__ = 'VK-Scraper==2.0.3'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('VK-Scraper==2.0.3', 'console_scripts', 'vk-scraper')()
    )
