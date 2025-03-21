'''Constants'''

from datetime import datetime, timezone, timedelta
from os.path import expanduser

HOME = expanduser("~")

try: 
    import ModulesPyOnly.plpyemul  as plpyemul #this library is not in PG
except:
    PG_ENVIRONMENT = True
    PY_ENVIRONMENT = False
else:
    PG_ENVIRONMENT = False
    PY_ENVIRONMENT = True

ERROR_POST_AUTHOR_NOT_FOUND  = 'Post author not found'
ERROR_POST_AUTHOR_EMPTY      = 'Post author empty'
ERROR_POST_DATE_NOT_FOUND    = 'Post date not found'
ERROR_POST_DATE_EMPTY        = 'Post date empty'
ERROR_REPLY_AUTHOR_NOT_FOUND = 'Reply author not found'
ERROR_REPLY_AUTHOR_EMPTY     = 'Reply author empty'
ERROR_REPLY_DATE_NOT_FOUND   = 'Reply date not found'
ERROR_REPLY_DATE_EMPTY       = 'Reply date empty'
ERROR_SCRAP_NUMBER_SUBSCRIBERS = 'Group number of subscribers not found'
ERROR_REQUEST_GET           = 'Get-request failed'
ERROR_REQUEST_POST          = 'Post-request failed'
ERROR_REQUEST_READ_TIMEOUT  = 'Read time-out error'
ERROR_DATE_RECOGNIZE        = 'Date not recognize'
ERROR_CONNECTION            = 'Connection error'

LOG_INFO_REQUEST_PAUSE      = 'Request pause'
LOG_INFO_DATA_PROCESSOR     = 'Data processor'

LOG_INFO_JOB_START          = 'Job start'
LOG_INFO_JOB_FINISH         = 'Job finish'
LOG_INFO_JOB_STEP_START     = 'Job step start'
LOG_INFO_JOB_RELOAD_PROGRAM = 'Job program reload'

CW_RESULT_TYPE_NUM_SUBSCRIBERS  = 'NUM_SUBSCRIBERS'
CW_RESULT_TYPE_NUM_SUBSCRIBERS_NOT_FOUND = 'NUM_SUBSCRIBERS Not found'
CW_RESULT_TYPE_FINISH_NOT_FOUND = 'FINISH Not found'
CW_RESULT_TYPE_FINISH_SUCCESS   = 'FINISH Success'
CW_RESULT_TYPE_ERROR            = 'ERROR'
CW_RESULT_TYPE_CRITICAL_ERROR   = 'CRITICAL ERROR'
CW_RESULT_TYPE_DB_ERROR         = 'ERROR DB WRITE\READ'
CW_RESULT_TYPE_WARNING          = 'WARNING'
CW_RESULT_TYPE_HTML             = 'HTML'
CW_RESULT_TYPE_POST             = 'POST'
CW_RESULT_TYPE_REPLY            = 'REPLY'
CW_RESULT_TYPE_REPLY_TO_REPLY   = 'REPLY to REPLY'
CW_RESULT_TYPE_DT_POST_ACTIVITY  = 'POST Last dt activity'
CW_RESULT_TYPE_DT_GROUP_ACTIVITY = 'GROUP Last dt activity'

CG_RESULT_TYPE_GROUPS_LIST  = 'GROUPS LIST'
CG_RESULT_TYPE_ERROR            = 'ERROR'
CG_RESULT_TYPE_CRITICAL_ERROR   = 'CRITICAL ERROR'
CG_RESULT_TYPE_DB_ERROR         = 'ERROR DB WRITE\READ'

LOG_LEVEL_FUNC = ['trace', 'debug', 'info', 'warn', 'error', 'fatal']
LOG_LEVEL_TRACE = 0
LOG_LEVEL_DEBUG = 1
LOG_LEVEL_INFO  = 2
LOG_LEVEL_WARN  = 3
LOG_LEVEL_ERROR = 4
LOG_LEVEL_FATAL = 5

HTTP_STATUS_CODE_200 = 200

SN_GROUP_MARK = 'G'
SN_USER_MARK = 'U'

#PARSING SECTION

SENTIM_RATE = {'negative':1,'neutral':2,'positive':3,'skip':4,'speech':5, 'error':99}


#COMMON SECTION

EMPTY_DATE = datetime(1,1,1)
EMPTY_DATE_UTC = datetime(1,1,1,tzinfo = timezone.utc)
HOURS_TZ_UTC_OFFSET = -3
LOCAL_TZ_UTC_OFFSET = timedelta(hours = HOURS_TZ_UTC_OFFSET)


if PG_ENVIRONMENT:
    TOKEN_FOLDER = '/opt2/pgpython/modules/Tokens/'
else:
    TOKEN_FOLDER = 'C:\\Temp\\'

if PG_ENVIRONMENT:
    LOG_FOLDER = '/opt2/pgpython/modules/Logs/'
else:
    LOG_FOLDER = '.\\Logs\\'
