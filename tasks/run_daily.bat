del %CD%\tasks\daily_log_prev.txt
move %CD%\tasks\daily_log.txt %CD%\tasks\daily_log_prev.txt
call set_env.bat
call %PDI_PATH%\kitchen.bat -file:%CD%\master\run_daily.kjb "-param:k84path=z:\Input" -logfile:%CD%\tasks\daily_log.txt
call %PDI_PATH%\kitchen.bat -file:%CD%\util\export\run_exports.kjb "-param:destPath=%CD%\util\export\results"
