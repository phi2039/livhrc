call set_env.bat
call %PDI_PATH%\kitchen.bat -file:%CD%\master\run_daily.kjb "-param:k84path=z:\Input" -logfile:%CD%\tasks\daily_log.txt
REM call %PDI_PATH%\kitchen.bat -file:%CD%\master\send_log.kjb "-param:recipEmail=clance@livingstonintl.com"
call %PDI_PATH%\kitchen.bat -file:%CD%\util\export\run_exports.kjb "-param:destPath=%CD%\util\export\results"
