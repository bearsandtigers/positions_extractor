import requests, json, time, re
from hashlib import md5
import asyncio
import aiohttp
import sys

import redis

search_pattern = 'удал|remot'
antipattern = 'удаление|удаленных'
keyword = sys.argv[1]


vacancies_max_num = 1999 # HH.RU's API allows up to 2000 vacancies only
vacancies_per_page = 99  # ...again, this is upper limit posed by HH.RU
pages_num = vacancies_max_num // vacancies_per_page or vacancies_max_num
#pages_num = 2
 
age=9
vacancies = f'https://api.hh.ru/vacancies?text={keyword}&per_page=\
            {vacancies_per_page}&period={age}&vacancy_search_order=publication_date\
            &page='
api_vacancy_url = 'https://api.hh.ru/vacancies/'
vacancy_url = 'https://hh.ru/vacancy/'

seen_list = set() # to avoid showing same vacancies multiple times
skipped_num = 0
n = nn = 0

excerpt_chars_num = 20
excerpt_line = ''

rds = redis.StrictRedis(host='localhost', port=6379,
                  charset="utf-8", decode_responses=True)
rds.flushall()

class Seen:
	'''
	Filter out reccuring vacancies (i.e. with similar descriptions)
	'''
	def __init__(self):
		self.seen_list = set()
		self.skipped = 0
		
	def seen(self, text):
		h = md5(text.encode()).hexdigest()
		if h in self.seen_list:
		    print(f"seen before, skip({self.count()})")
		    self.skipped += 1 
		    return True
		self.seen_list.add(h)
		return False
		
	def stat(self):
		print(f"Seen: {self.count()}, skipped: {self.skipped }")

	def count(self):
		return len(self.seen_list)

def rds_add(slice_idx, idx, v_info, excerpt_line='', reason=''):
	rds.hmset(f"{slice_idx}.{idx}", 
	           { 'info': json.dumps(v_info),
	             'excerpt': excerpt_line,
	             'reason': reason })

async def process_page(page_number):
	'''
	Receives page number and extracts and puts into Redis all 
	corresponding vacancies
	'''
	n = 0
	i = page_number
	print(f'Pages block №: {i}')
	async with aiohttp.ClientSession() as session:
		response = await session.get(vacancies + str(i))
		print(f'Async responce of page {i} received !')
		js = await response.json()
#		js = await session.get(vacancies + str(i)).json()
		print(f'Page {i} JSON parsed !')
		await session.close()
#	js = await response.json()
	for j in js['items']:
		n = n + 1
		v_id = j['id']
		async with aiohttp.ClientSession() as session:
			response = await session.get(api_vacancy_url + str(v_id))
			v = await response.json()
			await session.close()
		d = v['description']
		s = re.search(search_pattern, j['name'])
		if s:
			rds_add(i, n, j, '', 'Found in the title')
			continue

		v_type = v['schedule']['id']
		if  v_type == 'remote':
			rds_add(i, n, j, '', 'Type (' + v_type + ')' )
			continue

		d = re.sub('<[^<]+?>', '', d) # remove HTML tags
		d = re.sub(antipattern, '', d)
		s = re.search(search_pattern, d) # look for the pattern
		if s:
			s_start = s.start() - excerpt_chars_num
			if s_start < 0: s_start = 0
			s_end = s_start + ( excerpt_chars_num * 2 )
			if s_end > ( len(d) - 1 ): s_end = len(d) - 1
			
			print(f'\n{i}.Pattern found at {s.start()} (string len {len(d)}),\
			      slicing from {s_start} to {s_end}\n')

			excerpt_line = d[s_start:s_end]
			excerpt_line = '<... ' + excerpt_line + ' ...>'
			rds_add(i, n, j, excerpt_line, '')
	print(f'№ {page_number} finished')

def print_out():
	'''
	Extracts from Reddis and prints out vacancies put there by process_page()
	'''
	seen_checker = Seen()
	print('\n', ' ' * 20, 'Forming HTML\n')
	nn = 0
	output_html = 'a_remote_jobs.html'
	f = open(output_html, 'w')
	f.write('<head>\n<meta charset="UTF-8">\n\
	         <link rel="stylesheet" \
	         href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css"\
	         integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T"\
	         crossorigin="anonymous">\n\
	         </head>\n<body>\n\
	         <div class="wrapper container">\n')
	for k in rds.keys():
#		print(f'Key: {k}')
		info = json.loads(rds.hgetall(k)['info'])
		v_resp = info['snippet']['responsibility']
		v_id = info['id']
		v_name = info['name']
#		print(v_name)
		if seen_checker.seen(f"{v_name} {v_resp}"): continue
		v_empl = info['employer']['name']
		v_city = info['area']['name']
		v_salary = info['salary']
		v_creat = info['created_at'].split('T')[0]
		v_publish = info['published_at'].split('T')[0]
		v_date = f'{v_creat} / {v_publish}'
		if v_creat == v_publish: v_date = v_creat
		excerpt = rds.hgetall(k)['excerpt']
		reason = rds.hgetall(k)['reason']

		nn += 1
		if reason: v_name += r'    <<< SPECIFIED BY EMPLOYER >>>'
		v_url = vacancy_url + str(v_id)
		line = f'<div>\n<strong>{nn}.'\
			   f'<a target=_blank href={v_url}>{v_name}</a></strong>\t({v_salary})\n,'\
			   f'<br><mark>{v_empl}</mark> ({v_city}),'\
			   f'<br><small>{v_date}</small><br>'
		if excerpt: line +=  '<i>' + excerpt + '</i><br>'
		line += '</div><br>\n'
		f.write(line)
		f.flush()

	f.write('</div></body>')
	f.close()
	seen_checker.stat()
	
async def asynchronous():
	tasks = [asyncio.ensure_future(
			process_page(page)) for page in range(pages_num)]
	await asyncio.wait(tasks)

def main():
	print('Main')
	ioloop = asyncio.get_event_loop()
	ioloop.run_until_complete(asynchronous())
	ioloop.close()
	print_out()

if __name__ == "__main__":
	main()
