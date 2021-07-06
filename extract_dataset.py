#!/usr/bin/env python

import arrow
import os
import csv
import sys
from elasticsearch_dsl import Search, Q
from elasticsearch import Elasticsearch
from functools import reduce, lru_cache
from operator import or_

FIELDS = [
    'providers',
    'source_doi',
    'source_doi_url',
    'source_identifiers',
    'source_urls',
    'source_type',
    'source_title',
    'source_publisher',
    'source_publication_year',
    'source_domain',
    'source_version_id',
    'target_doi',
    'target_doi_url',
    'target_identifiers',
    'target_urls',
    'target_type',
    'target_title',
    'target_publisher',
    'target_publication_year',
    'target_domain',
    'target_version_id',
]


# client = Elasticsearch(hosts=[{
#     'host': 'zenodo-broker-qa-search.cern.ch',
#     'port': 443,
#     'http_auth': ('broker', os.environ.get('ES_PASS')),
#     'use_ssl': True,
#     'verify_certs': False}
# ])

from invenio_search import current_search_client
client = current_search_client


def first(it):
    return next(it, None)


def parse_date(d):
    if d and len(d) == 4:
        return int(d)
    else:
        return arrow.get(d).year


VERSION_QUERY = (
    Search(index='relationships', using=client)
    .filter('term', Grouping='version')
    .filter('term', RelationshipType='Cites'))


@lru_cache(maxsize=5000)
def fetch_version_id(identifier):
    for path in ('Target', 'Source'):
        search = VERSION_QUERY.filter(
            Q('nested', path=f'{path}.SearchIdentifier',
              query={'term': {f'{path}.SearchIdentifier.ID': identifier}}))
        res = search[0:1].execute()
        if res:
            return res[0][path]['ID']
        search = VERSION_QUERY.filter(
            Q('nested', path=f'{path}.SearchIdentifier',
              query={'term': {f'{path}.SearchIdentifier.ID': identifier}}))
        res = search[0:1].execute()
        if res:
            return res[0][path]['ID']


def object_data(obj):
    doi = first(i for i in obj.Identifier if i.IDScheme.lower() == 'doi')
    version_id = fetch_version_id(doi.ID) if doi else None
    if not version_id:
        for i in obj.Identifier:
            version_id = fetch_version_id(i.ID)
            if version_id:
                break
    return {
        'doi': doi.ID if doi else None,
        'doi_url': doi.IDURL if doi else None,
        'identifiers': ';'.join(i.ID for i in obj.Identifier),
        'urls': ';'.join(i.IDURL for i in obj.Identifier if 'IDURL' in i),
        'title': obj.Title if 'Title' in obj else None,
        'type': obj.Type.Name if 'Type' in obj else None,
        'publisher': first(p.Name for p in obj.Publisher) if 'Publisher' in obj else None,
        'publication_year': parse_date(obj.PublicationDate) if 'PublicationDate' in obj else None,
        'domain': None,
        'version_id': version_id,
    }


def extract_providers(history):
    return list(reduce(
        or_, [{lp.Name for lp in h.LinkProvider} for h in history], set()))


def fetch_results():
    results = []
    search = Search(index='relationships', using=client)
    search = search.filter('term', Grouping='identity')  # only identity groups
    search = search.filter('term', RelationshipType='Cites')  # only citations
    for rel in search.scan():
        source_data = object_data(rel.Source)
        target_data = object_data(rel.Target)
        providers = ';'.join(extract_providers(rel.History))
        results.append({
            **{f'source_{k}': v for k, v in source_data.items()},
            **{f'target_{k}': v for k, v in target_data.items()},
            **{'providers': providers},
        })
    return results


if __name__ == "__main__":
    fname = f'{sys.argv[1]}.csv'
    print(fname)
    results = fetch_results()
    with open(fname, 'w') as fp:
        writer = csv.DictWriter(fp, FIELDS)
        writer.writeheader()
        writer.writerows(results)
    print(len(results))
