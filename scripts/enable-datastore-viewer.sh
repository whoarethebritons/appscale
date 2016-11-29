#!/bin/bash
#
# author: g@appscale.com
#
# Enable the datastore viewer and reload nginx.

ALLOW=""
APP_ID=""
IP="all"
VIEWER_PORT="8100"
LOCAL_PORT="30000"

usage() {
        echo
        echo "Usage: $0 [app-id ...]"
        echo
        echo "Enable the dataviewer for app-id. If no app-id is specified, enable the viewer for all apps."
        echo "WARNING: the datastore viewer is not protected! Anyone can browse your data."
        echo "WARNING: restricting by IP should be used judiciously."
        echo
        echo "Options:"
        echo "     -h        this message"
        echo "     -i <IP>   allow connections only from this IP (default is open)"
        echo
}

while [ $# -gt 0 ]; do
        if [ "$1" = "-h" -o "$1" = "-help" -o "$1" = "--help" ]; then
                usage
                exit 1
        fi
        if [ -n "$1" -a "$1" = "-i" ]; then
                if [ -n "$2" ]; then
                        IP="$2"
                        ALLOW="allow $IP;
      deny all;"
                        shift;shift
                        continue
                else
                        usage
                        exit 1
                fi
        fi
        if [ -n "$1" ]; then
                APP_ID=$1
                shift
                continue
        fi
done

# Sanity checks.
if [ ! -e /etc/nginx/sites-enabled ]; then
        echo "ERROR: Cannot find nginx configurations. Is this an AppScale deployment?"
        exit 1
fi

APPENGINE_IP=""
for ip in $(cat /etc/appscale/all_ips); do
        OUTPUT=$(ssh $ip -i /etc/appscale/keys/cloud1/*.key 'ps ax | grep appserver \
               | grep -E "(grep|$APP_ID)" | grep -- "--admin_port" \
               | sed "s;.*--admin_port \([0-9]*\).*/var/apps/\(.*\)/app .*;\1 \2;g" \
               | sort -ru')
        for i in $OUTPUT ; do
                if [ "$i" = "$APP_ID" ]; then
                        continue
                else
                        port=$i
                        APPENGINE_IP=$ip
                        break
                fi
        done
done

while [[ $(lsof -i :$VIEWER_PORT) ]]; do
        let $((VIEWER_PORT += 1))
done

while [[ $(lsof -i :$LOCAL_PORT) ]]; do
        let $((LOCAL_PORT += 1))
done

NO_OF_IP_LINES=$(wc -l < /etc/appscale/all_ips)
if [ "$NO_OF_IP_LINES" = "2" ]; then
        LOCAL_PORT=$port
fi

# Prepare the nginx config snippet.
config="
upstream datastore_viewer_$VIEWER_PORT {
  server localhost:$LOCAL_PORT;
}
map \$scheme \$ssl {
    default off;
    https on;
}

server {
    listen $VIEWER_PORT;
    server_name datastore_viewer_server;
    location / {
      $ALLOW
      proxy_pass http://datastore_viewer_$VIEWER_PORT;
    }
}
"

if [ -e /etc/nginx/sites-enabled/appscale-${APP_ID}.conf ]; then
        echo "$config" > /etc/nginx/sites-enabled/appscale-${APP_ID}_datastore_viewer.conf
        service nginx reload
        echo "Datastore viewer enabled for ${APP_ID} at http://$(cat /etc/appscale/my_public_ip):${VIEWER_PORT}. Allowed IP: $IP."
        if [ "$NO_OF_IP_LINES" != "2" ]; then
                echo "Note: For a multi node deployment, you will need to forward the admin port from one of the AppServers on another node to the head node."
                echo "Modify the appscale/firewall.conf to open ports for the datastore viewer."
                echo "Run the SSH Tunnelling command: ssh -L ${LOCAL_PORT}:localhost:${port} ${APPENGINE_IP} -N"
                echo "You might need to type in the password for the app engine node."
        fi
else
        echo "Cannot find configuration for ${APP_ID}."
fi
