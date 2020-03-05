import os
from datetime import datetime
import time
current=datetime.now().strftime("%H:%M:%S")
a=10
b=02
c=0
now =datetime.now()
hours=now.hour
mins=now.minute
sec=now.second
in_sec=c+60*(b+(60*a))
now_sec=sec+60*(mins+(60*hours))
times=now_sec-in_sec
rem_hour=times//3600
times%=3600
rem_min=times//60
times%=60
rem_sec=times
while (True):
	if(times==60):
		rem_min=rem_min+1
		times=00
	if(rem_min==60):
		rem_hour=rem_hour+1
		rem_min=00
	times=times+1
	time.sleep(1)
	t="{}:{}:{}"
	print(t.format(rem_hour,rem_min,times))















