import requests
from geopy.geocoders import GoogleV3
import os
import sys
import json
import datetime
import time
import pandas as pd
from itertools import izip

def push(user, passwd, baseurl, homes, waittime=0.05, geocode_status='failsafe'):
	# After this many retries in failsafe mode, give up on geocoding client side
	failsafe_retries = 5

	# For every single entry, do this many retries before moving on
	line_retries = 2

	# If you have too many failures in a row without any success, stop.  Reset to this count on one success.
	ceiling_overall_retries = 10

	r = requests.post(baseurl + '/token/auth/', data={"username":user, "password":passwd})
	j = r.json()
	if r.status_code != 200:
		if 'non_field_errors' in j.keys():
			errors = j['non_field_errors']
			return {"errors": errors}
		else:
			return {"errors": ['Cannot get token from API']}

	if not('token' in j.keys()):
		return {"errors": "ERROR: Got 200 response but it does not contain a token"}

	token = j['token']

	if geocode_status != 'none':
		encoder = GoogleV3()

	overall_retries = ceiling_overall_retries
	fail_count = 0
	giveup_count = 0
	success_count = 0

	result = {"start": str(datetime.datetime.now())}
	for home in homes:
		if geocode_status != 'none' and failsafe_retries >= 0:
			try:
				location = encoder.geocode(home['raw_address'])
				home['geocoded_address'] = location.address
				home['lat'] = location.latitude
				home['lon'] = location.longitude
				home['rawjson'] = location.raw
			except:
				failsafe_retries -= 1
				if geocode_status == 'failsafe' and failsafe_retries < 0:
					geocode_status = 'none'
				if geocode_status == 'failsecure':
					print("ERROR: Problems with geocoding")
					sys.exit(1)
		retries = line_retries
		failure = True
		while retries >= 0 and overall_retries >= 0 and failure:
			failure = False
			try:
				headers = {"Authorization": "Bearer " + token}
				time.sleep(waittime)
				p = requests.post(baseurl + '/api/property/', data=home, headers=headers)
				if p.status_code == 201:
					success_count += 1
				else:
					failure = True
					fail_count += 1
				overall_retries = ceiling_overall_retries
			except UnboundLocalError:
				failure = True
			if failure:
				retries -= 1
				overall_retries -= 1
				msg = "ERROR [line " + str(i) + "]"
				if retries >= 0 and overall_retries >= 0:
					msg += " (going to retry)\n"
				msg += json.dumps(data).replace('\n', '')
				terminal_error = False
				try:
					detail = json.loads(p.content)
					if 'detail' in detail.keys() and detail['detail'] == 'You do not have permission to perform this action.':
						msg = 'Your account does not have write access.  Please contact kyle@dataskeptic.com'
						print(msg)
						terminal_error = True
				except:
					pass
				if terminal_error:
					sys.exit(1)
		if failure:
			giveup_count += 1
		if overall_retries < 0:
			msg = "Quitting due to too many failures\n"
			print(msg)

	msg = "Successfully uploaded: " + str(success_count)
	print(msg)
	msg = "Failures experienced: " + str(fail_count)
	print(msg)
	msg = "Unrecovered failures: " + str(giveup_count)
	print(msg)
	return result
