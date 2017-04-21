import requests
from bs4 import BeautifulSoup
import time
import populate
import pandas as pd
import sys

def run(query):
	r = requests.get('https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population')
	soup = BeautifulSoup(r.content)
	#
	tbl = soup.find_all('table')[3]
	df = pd.read_html(str(tbl))[0]
	#
	df.columns = df.iloc[0]
	#
	cities = df['City'].tolist()
	#
	for city in cities:
		i = city.find('[')
		if i != -1:
			city = city[0:i]
		city = city + ' ' + query
		print(city)
		populate.query_and_post(city)
		time.sleep(1)

if __name__ == "__main__":
	query = sys.argv[1]
	run(query)
