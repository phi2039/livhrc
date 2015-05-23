call set_env.bat
call %PDI_PATH%\kitchen.bat -file:%CD%\master\run_daily.kjb "-param:k84path=z:\Input" -logfile:%CD%\tasks\daily.log
call %PDI_PATH%\kitchen.bat -file:%CD%\master\run_daily.kjb "-param:recipEmail=clance@livingstonintl.com"