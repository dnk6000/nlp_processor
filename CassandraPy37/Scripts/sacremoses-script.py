#!C:\Work\Python\CasCrawl37\CassandraPy37\Scripts\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'sacremoses==0.0.35','console_scripts','sacremoses'
__requires__ = 'sacremoses==0.0.35'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('sacremoses==0.0.35', 'console_scripts', 'sacremoses')()
    )
