import sys
import json
import base64
from lxml import etree

from pyspark import SparkContext

def put_into_map(elem, attr, m):
  try:
    attr_value = elem.get(attr)
    if attr:
      m[attr] = attr_value
    else:
      m[attr] = "null"
  except Exception, e:
    m[attr] = "null"

def _extract_field(xpage):
  try:
    m = {}
    data = xpage.replace("UTF-16", "UTF-8")
    e_root = etree.XML(data) 
    
    e_pageinfo = e_root.find("pageinfo") 
    put_into_map(e_pageinfo, "page-type", m)
    put_into_map(e_pageinfo, "last-modified", m)
    put_into_map(e_pageinfo, "pagesize", m)
    
    e_flags = e_root.find("flags")
    put_into_map(e_flags, "best-ans-len", m)
    put_into_map(e_flags, "reply-num", m)
    put_into_map(e_flags, "ans-num", m)
    
    str_breadcrumb = "null"
    try:
      e_breadcrumb = e_root.find("content").find("breadcrumb")
      str_breadcrumb = e_breadcrumb.text.encode("utf8")
    except Exception, e:
      pass
    m["breadcrumb"] = str_breadcrumb
    return m
  except Exception, e:
    return None

def extract_field(x):
  fetch_time = x[0].strip()
  url = x[1].strip()
  xpage = base64.b64decode(x[2].strip()) #xpage is utf8
  fields = _extract_field(xpage)
  if fields != None:
    fields["fetch-time"] = fetch_time
    fields["url"] = url
  return fields

INPUT  = sys.argv[1]
OUTPUT = sys.argv[2]

sc = SparkContext()

rdd = sc.textFile(INPUT, use_unicode=False) \
  .map(lambda x: x.strip().split("\t")) \
  .filter(lambda x: len(x) == 4) \
  .map(lambda x: (x[0], x[1], x[2]) ) \
  .map(extract_field) \
  .filter(lambda x: x!=None) \
  .map(lambda x: json.dumps(x, ensure_ascii=False))

rdd.repartition(100).saveAsTextFile(OUTPUT)

sc.stop()
