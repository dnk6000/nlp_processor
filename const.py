'''Constants'''

from datetime import datetime

try:
    plpy.execute('SELECT 1')
except:
    PG_ENVIRONMENT = False
else:
    PG_ENVIRONMENT = True

ERROR_POST_AUTHOR_NOT_FOUND  = 'Post author not found'
ERROR_POST_AUTHOR_EMPTY      = 'Post author empty'
ERROR_POST_DATE_NOT_FOUND    = 'Post date not found'
ERROR_POST_DATE_EMPTY        = 'Post date empty'
ERROR_REPLY_AUTHOR_NOT_FOUND = 'Reply author not found'
ERROR_REPLY_AUTHOR_EMPTY     = 'Reply author empty'
ERROR_REPLY_DATE_NOT_FOUND   = 'Reply date not found'
ERROR_REPLY_DATE_EMPTY       = 'Reply date empty'
ERROR_SCRAP_NUMBER_SUBSCRIBERS = 'Group number of subscribers not found'
ERROR_REQUEST_GET  = 'Get-request failed'
ERROR_REQUEST_POST = 'Post-request failed'

CW_RESULT_TYPE_NUM_SUBSCRIBERS  = 'NUM_SUBSCRIBERS'
CW_RESULT_TYPE_NUM_SUBSCRIBERS_NOT_FOUND = 'NUM_SUBSCRIBERS Not found'
CW_RESULT_TYPE_FINISH_NOT_FOUND = 'FINISH Not found'
CW_RESULT_TYPE_FINISH_SUCCESS   = 'FINISH Success'
CW_RESULT_TYPE_ERROR            = 'ERROR'
CW_RESULT_TYPE_CRITICAL_ERROR   = 'CRITICAL ERROR'
CW_RESULT_TYPE_DB_ERROR         = 'ERROR DB WRITE\READ'
CW_RESULT_TYPE_HTML             = 'HTML'
CW_RESULT_TYPE_POST             = 'POST'
CW_RESULT_TYPE_REPLY            = 'REPLY'
CW_RESULT_TYPE_REPLY_TO_REPLY   = 'REPLY to REPLY'
CW_RESULT_TYPE_DT_POST_ACTIVITY  = 'POST Last dt activity'
CW_RESULT_TYPE_DT_GROUP_ACTIVITY = 'GROUP Last dt activity'

CW_LOG_LEVEL_FUNC = ['trace', 'debug', 'info', 'warn', 'error', 'fatal']
CW_LOG_LEVEL_TRACE = 0
CW_LOG_LEVEL_DEBUG = 1
CW_LOG_LEVEL_INFO  = 2
CW_LOG_LEVEL_WARN  = 3
CW_LOG_LEVEL_ERROR = 4
CW_LOG_LEVEL_FATAL = 5

EMPTY_DATE = datetime(1,1,1)

SN_GROUP_MARK = 'G'
SN_USER_MARK = 'U'