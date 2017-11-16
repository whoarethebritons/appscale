#!/usr/bin/env bash

set -e
set -u

wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | apt-key add -
apt-get install apt-transport-https
echo "deb https://artifacts.elastic.co/packages/5.x/apt stable main" | tee -a /etc/apt/sources.list.d/elastic-5.x.list
apt-get update && apt-get install logstash
systemctl enable logstash.service
systemctl start logstash.service

ES_IP="104.198.135.65"

cat > /etc/logstash/conf.d/requests.conf << LOGSTASH_CONF

input {
  http {
    port => 31313
  }
}

filter {
  mutate {
    id => "%{startTime}-%{[requestId]}"
    remove_field => "headers"
  }
  date {
    match => [ "[startTime]", "UNIX_MS" ]
  }
  date {
    match => [ "[endTime]", "UNIX_MS" ]
  }
}

output {
  elasticsearch {
    hosts => "${ES_IP}:9200"
    manage_template => false
    index => "%{[appId]}-%{[moduleName]}-%{+YYYY.MM.dd}"
    document_type => "request"
  }
}

LOGSTASH_CONF


cat > /etc/logstash/conf.d/log-entries.conf << LOGSTASH_CONF

input {
  http {
    port => 31314
  }
}

filter {
  split {
    field => "appLogs"
  }
  mutate {
    rename => [
      "[appLogs][orderKey]", "orderKey",
      "[appLogs][requestId]", "requestId",
      "[appLogs][time]", "@timestamp",
      "[appLogs][level]", "level",
      "[appLogs][message]", "message"
    ]
    remove_field => ["appLogs", "headers"]
  }
  date {
    match => [ "[@timestamp]", "UNIX_MS" ]
  }
}

output {
  elasticsearch {
    hosts => "${ES_IP}:9200"
    manage_template => false
    index => "%{[appId]}-%{[moduleName]}-%{+YYYY.MM.dd}"
    document_type => "log-entry"
  }
}

LOGSTASH_CONF
