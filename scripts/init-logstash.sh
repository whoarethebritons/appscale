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

cat > /etc/logstash/conf.d/logstash.conf << LOGSTASH_CONF

input {
  http {
    port => 31313
  }
}

filter {

  if ([startTime]) {
    mutate {
      id => "%{[startTime]}-%{[requestId]}"
      remove_field => ["headers"]
    }
    date {
      match => [ "[startTime]", "UNIX_MS" ]
      target => "startTime"
    }
    date {
      match => [ "[endTime]", "UNIX_MS" ]
      target => "endTime"
    }
  }

  if ([appLogs]) {
    split {
      field => "appLogs"
    }
   date {
     match => [ "[appLogs][time]", "UNIX_MS" ]
     target => "[appLogs][time]"
   }
   mutate {
      rename => [
        "[appLogs][orderKey]", "orderKey",
        "[appLogs][requestId]", "requestId",
        "[appLogs][time]", "@timestamp",
        "[appLogs][level]", "level",
        "[appLogs][message]", "message",
        "[appId]", "[@metadata][appId]",
        "[serviceName]", "[@metadata][serviceName]"
      ]
      remove_field => ["appLogs", "headers"]
    }
  }
}

output {

  stdout { codec => rubydebug}

  if ([startTime]) {
    elasticsearch {
      hosts => "104.198.135.65:9200"
      manage_template => false
      index => "app-%{[appId]}-%{[serviceName]}-%{+YYYY.MM.dd}"
      document_type => "request"
    }
  }

  if ([appLogs]) {
    elasticsearch {
      hosts => "104.198.135.65:9200"
      manage_template => false
      index => "app-%{[@metadata][appId]}-%{[@metadata][serviceName]}-%{+YYYY.MM.dd}"
      document_type => "logentry"
    }
  }
}

LOGSTASH_CONF
