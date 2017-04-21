from bs4 import BeautifulSoup
import requests
import dryscrape
import json
import sys


api = "https://5xwvsgjnqi.execute-api.us-east-1.amazonaws.com/prod/OH-submit-url"
url = 'https://duckduckgo.com/?q={}&ia=web'

def query_and_post(query):
	session = dryscrape.Session()
	#
	u = url.format(query.replace(' ', '%20'))
	session.visit(u)
	response = session.body()
	soup = BeautifulSoup(response)
	#
	links = soup.find_all('a', {'class': 'result__url'})
	filtered = []
	for link in links:
		href = link.get('href')
		if href.find('search.yahoo.com') == -1:
			filtered.append(href)
	#
	for link in filtered:
		res = {"email": "", "url": link, "checked": False}
		requests.post(api, data=json.dumps(res))

if __name__ == "__main__":
	query = sys.argv[1]
	query_and_post(query)