curl -XPUT 'http://elastic:changeme@elasticsearch:9200/catalog'
curl -XPOST 'http://elastic:changeme@elasticsearch:9200/catalog/dataset/_mapping' -d @./mappings.json

