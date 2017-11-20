#!/usr/bin/env bash

set -e
set -u


wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | apt-key add -
apt-get install apt-transport-https
echo "deb https://artifacts.elastic.co/packages/5.x/apt stable main" | tee -a /etc/apt/sources.list.d/elastic-5.x.list
apt-get update && apt-get install logstash
systemctl enable logstash.service
systemctl start logstash.service

ES_IP="localhost"

cat > /etc/logstash/conf.d/logstash.conf << LOGSTASH_CONF

input {
  http {
    port => 31313
    host => "0.0.0.0"
  }
}

filter {

  if ([startTime]) {
    date {
      match => [ "[startTime]", "UNIX_MS" ]
      target => "@timestamp"
    }
    date {
      match => [ "[endTime]", "UNIX_MS" ]
      target => "endTime"
    }
    mutate {
      id => "%{[generated_id]}"
      rename => [
        "[generated_id]", "[@metadata][generated_id]",
        "[appscale-host]", "[host]",
        "[appId]", "[@metadata][appId]",
        "[serviceName]", "[@metadata][serviceName]"
      ]
      remove_field => ["appLogs", "headers", "startTime"]
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
      id => "%{[appLogs][generated_id]}"
      rename => [
        "[appLogs][generated_id]", "[@metadata][generated_id]",
        "[appLogs][time]", "@timestamp",
        "[appLogs][level]", "level",
        "[appLogs][message]", "message",
        "[appId]", "[@metadata][appId]",
        "[serviceName]", "[@metadata][serviceName]"
      ]
      remove_field => ["appLogs", "headers", "host"]
    }
  }
}

output {

  stdout { codec => rubydebug}

  if ([endTime]) {
    elasticsearch {
      hosts => "${ES_IP}:9200"
      manage_template => false
      index => "app-%{[@metadata][appId]}-%{[@metadata][serviceName]}-%{+YYYY.MM.dd}"
      document_type => "request"
      document_id => "%{[@metadata][generated_id]}"
    }
  }

  if ([message]) {
    elasticsearch {
      hosts => "${ES_IP}:9200"
      manage_template => false
      index => "app-%{[@metadata][appId]}-%{[@metadata][serviceName]}-%{+YYYY.MM.dd}"
      document_type => "logentry"
      document_id => "%{[@metadata][generated_id]}"
    }
  }
}

LOGSTASH_CONF
