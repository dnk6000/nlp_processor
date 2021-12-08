import modules.common_mod.const as const

if const.PG_ENVIRONMENT: 
    import sys
    venv_path = '/opt2/pgpython/modules/ext_modules/'
    sys.path.append(venv_path)
    
