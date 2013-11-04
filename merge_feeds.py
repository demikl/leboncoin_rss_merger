#!/usr/bin/env python
# -*- coding: UTF8 -*-

import eventlet
feedparser = eventlet.import_patched('feedparser')
import requests
import PyRSS2Gen
import re, itertools, sys, datetime
from time import mktime


# URL directes
sources_feeds = [
]

# Annonces immobilières par code postal
codes_postaux = [ 49300, 49280, 49740, 85600, 85290, 85530, 85260, 49450, 49230, 49600, 44430, 44330, 44190 ]
url_prefix = 'http://lbc2rss.superfetatoire.com/www.leboncoin.fr/ventes_immobilieres/offres/pays_de_la_loire/?f=a&th=1&ps=6&pe=11&sqs=11&location=Toutes%20les%20communes%20'
sources_feeds.extend( [ "".join([ url_prefix, str(cp) ]) for cp in codes_postaux ] )


removeUnreachableContent = False	# delete items whose link is unreachable
removeDuplicateItems = False        # if multiple items have an equivalent "description" field, keep only the most recent one
setGuidFromDescription = True		# change GUID for each item and set it according to the description field

# Fetch source feeds
pool = eventlet.GreenPool()
feeds = []
for feed in pool.imap( feedparser.parse, sources_feeds ):
	feeds += [feed]

# regroupement des toutes les annonces
merged_feed = []
for feed in feeds:
	merged_feed.extend( feed['items'] )

if removeDuplicateItems:
	# Suppression de la reference de l'annonce de le titre afin de comparer deux annonces identiques
	title_regex = re.compile("(.*)  - ref:[0-9]+$")
	for item in merged_feed:
		useful = title_regex.match( item['title'] )
		if useful:
			item['title'] = useful.group(1)

	# Tri par titre d'annonce, pour trouver les duplicas
	feed = sorted( merged_feed, key=lambda item: item['title'] )

	# Filtre qui va supprimer des descriptions d'annonce les éléments changeants
	descr_regex = re.compile( """(.*)<img .* src="[^"]+" />(.*)<p><strong>Mise en ligne de l'annonce : </strong>.*</p>""" )

	# Regroupement des annonces ayant le meme titre
	for items in [ list(g) for k,g in itertools.groupby( feed, key=lambda item: item['title'] ) ]:
		if len(items) == 1 : continue
		# Extraction de la description en enlevant les éléments aléatoires
		for item in items:
			keep = descr_regex.match( item['summary'] )
			item['cleaned_summary'] = item['summary'] if not keep else "".join( [ keep.group(1), keep.group(2) ] )
		# Tri par description
		items_sorted = sorted( items, key=lambda item: item['cleaned_summary'] )
		print "Annonces similaires : ", [ i['cleaned_summary'] for i in items_sorted ]
		# Regroupement des annonces identiques
		for same_items in [ list(g) for k,g in itertools.groupby( items_sorted, key=lambda item: item['cleaned_summary'] ) ]:
			if len(same_items) == 1: continue
			# Tri des annonces identiques par récence
			same_items_sorted = sorted( items, key=lambda item: item['published'] ).reverse()
			# Marquage des annonces autres que la plus récente
			print "Annonces identiques : ", [ i['summary'] for i in same_items_sorted ]
			for item in same_items_sorted[1:]:
				item['purge'] = True
	# Suppression des annonces marquées
	merged_feed = [ item for item in merged_feed if not (item.has_key('purge') and item['purge'] == True) ]
		
if setGuidFromDescription:
	# Suppression de la reference de l'annonce de le titre afin de comparer deux annonces identiques
	title_regex = re.compile("(.*)  - ref:[0-9]+$")
	for item in merged_feed:
		useful = title_regex.match( item['title'] )
		if useful:
			item['title'] = useful.group(1)

	# Filtre qui va supprimer des descriptions d'annonce les éléments changeants
	descr_regex = re.compile( """(.*)<img .* src="[^"]+" />(.*)<p><strong>Mise en ligne de l'annonce : </strong>.*</p>""" )

	# Construction d'un ID à partir des éléments non changeant de la description
	for item in merged_feed:
		keep = descr_regex.match( item['summary'] )
		item['id'] = sys.maxint + hash( item['summary'] if not keep else "".join( [ keep.group(1), keep.group(2) ] ) )


if removeUnreachableContent:
	def check_status(item):
		return item, requests.head( item['link'] ).status_code

	purged_feed = []
	for item, status in pool.imap( check_status, merged_feed ):
		if status == 200: purged_feed += [item]
	merged_feed = purged_feed

# Sort by most recent first
sorted_feed = sorted( merged_feed, key=lambda item: item['published_parsed'] )
sorted_feed.reverse()


# Construction du flux XML de sortie
rss = PyRSS2Gen.RSS2(
	title = 'Annonces immobilieres personnalisees',
	link = 'http://www.leboncoin.fr',
	description = 'Regroupement de plusieurs recherches leboncoin',
	lastBuildDate = datetime.datetime.now(),
	items = [ PyRSS2Gen.RSSItem(
			title = item['title'],
			link  = item['link'],
			description = item['description'],
			guid = PyRSS2Gen.Guid( str(item['id']), isPermaLink=0 ),
			pubDate = datetime.datetime.fromtimestamp( mktime( item['published_parsed'] ) )
		)
		for item in sorted_feed ]
	)
rss.write_xml(open("leboncoin.xml", "w"))