from lxml import etree
import re, json
import mysql.connector

RE_MORPHOLOGY = re.compile(r'^{{морфо-ru\|(.*)}}$', re.MULTILINE)

XMLNS = '{http://www.mediawiki.org/xml/export-0.10/}'

IN_FILE = 'ruwiktionary-latest-pages-articles.xml.bz2'

def iter_posts(filename):
	if filename.endswith('.bz2'):
		import bz2
		open_method = bz2.open
	else:
		open_method = open
	with open_method(filename, 'rb') as fp:
		context = etree.iterparse(fp, events=('start','end'))
		parser = parse_empty
		for ev, elem in context:
			if ev == 'start':
				if elem.tag == XMLNS+'page':
					result = {}
					parser = parse_page(result)
				elif elem.tag == XMLNS+'revision':
					parser = parse_page_rev(result)
			elif ev == 'end':
				if elem.tag == XMLNS+'page':
					yield result
					parser = parse_empty
				elif elem.tag == XMLNS+'revision':
					parser = parse_page(result)

				parser(elem)
				elem.clear()
				while elem.getprevious() is not None:
					del elem.getparent()[0]

def parse_empty(elem):
	pass

def parse_page(result):
	def fn(elem):
		if elem.tag == XMLNS+'id':
			result['id'] = elem.text
		elif elem.tag == XMLNS+'title':
			result['title'] = elem.text
		elif elem.tag == XMLNS+'ns':
			result['ns'] = elem.text
	return fn

def parse_page_rev(result):
	def fn(elem):
		if elem.tag == XMLNS+'text':
			if elem.text:
				morphology = RE_MORPHOLOGY.findall(elem.text)
				morphology = [m for m in morphology if m]
				morphology = list(set(morphology))
				result['morphology'] = ';'.join(morphology)
			else:
				result['morphology'] = ''
		if elem.tag == XMLNS+'id':
			result['rev_id'] = elem.text
	return fn

connection = mysql.connector.connect(user='wikidata', password='', host='127.0.0.1', database='wikidata', port=3406)
cursor = connection.cursor()
cursor.execute('DROP TABLE IF EXISTS `articles`')
cursor.execute('''CREATE TABLE `articles` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `ns` int(11) DEFAULT NULL,
  `title` text CHARACTER SET utf8mb4 NOT NULL,
  `revision` bigint(20) DEFAULT NULL,
  `morphology` text CHARACTER SET utf8mb4,
  `morphology-datuum` text CHARACTER SET utf8mb4,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4''')
connection.commit()

def transform_morpheme(e):
	e = e.replace('j', '')
	if e.startswith('и='):
		# source
		return None
	elif e == '':
		return None
	elif e.startswith('+'):
		# ending
		return {'m': 'ending', 'p': [0, len(e)-1], 'w': e[1:]}
	elif e.startswith('-') and e.endswith('-'):
		# interfix
		return {'m': 'interfix', 'p': [0, len(e)-2], 'w': e[1:-1]}
	elif e.startswith('--'):
		# suffixoid
		return {'m': 'suffix', 'p': [0, len(e)-2], 'w': e[2:]}
	elif e.endswith('--'):
		# prefixoid
		return {'m': 'prefix', 'p': [0, len(e)-2], 'w': e[:-2]}
	elif e.startswith('-'):
		# suffix
		return {'m': 'suffix', 'p': [0, len(e)-1], 'w': e[1:]}
	elif e.endswith('-'):
		# prefix
		return {'m': 'prefix', 'p': [0, len(e)-1], 'w': e[:-1]}
	else:
		# root
		return {'m': 'root', 'p': [0, len(e)], 'w': e}

def transform(morphology):
	elements = []
	offset = 0
	for e in morphology.split('|'):
		morpheme = transform_morpheme(e)
		if morpheme is None:
			continue
		len = morpheme['p'][1]
		morpheme['p'] = [offset, offset+len]
		elements.append(morpheme)
		offset += len
	return elements

C = 0
for page in iter_posts(IN_FILE):
	m = page['morphology']
	if m:
		m = m.split(';')[0]
		m = transform(m)
		m = json.dumps(m, ensure_ascii=False)
		cursor.execute('INSERT INTO `articles` (`id`, `ns`, `title`, `revision`, `morphology`, `morphology-datuum`) VALUES (%s, %s, %s, %s, %s, %s)', (page['id'], page['ns'], page['title'].encode('utf8'), page['rev_id'], page['morphology'].encode('utf8'), m))
		connection.commit()
		print('{}      '.format(C), end='\r')
		C += 1
cursor.close()
connection.close()
