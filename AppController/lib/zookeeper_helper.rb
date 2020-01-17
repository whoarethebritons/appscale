require 'djinn'

# The location on the local filesystem where we should store ZooKeeper data.
DATA_LOCATION = '/opt/appscale/zookeeper'.freeze

# The path in ZooKeeper where the deployment ID is stored.
DEPLOYMENT_ID_PATH = '/appscale/deployment_id'.freeze

# Name for service as per helper.
ZOOKEEPER_SERVICE_NAME = "appscale-zookeeper.target".freeze

def configure_zookeeper(nodes, my_index)
  # TODO: create multi node configuration
  zoocfg = <<ZOOCFG
tickTime=2000
initLimit=10
syncLimit=5
dataDir=#{DATA_LOCATION}
clientPort=2181
leaderServes=yes
maxClientCnxns=0
forceSync=no
skipACL=yes
autopurge.snapRetainCount=5
# Increased zookeeper activity can produce a vast amount of logs/snapshots.
# With this we ensure that logs/snapshots are cleaned up hourly.
autopurge.purgeInterval=1
ZOOCFG
  myid = ''

  zoosize = nodes.count { |node| node.is_zookeeper? }

  if zoosize > 1
    # from 3.4.0, server settings is valid only in two or more nodes.
    zooid = 1
    nodes.each_with_index { |node, index|
      next unless node.is_zookeeper?
      zoocfg += <<ZOOCFG
server.#{zooid}=#{node.private_ip}:2888:3888
ZOOCFG
      myid = zooid.to_s if index == my_index
      zooid += 1
    }
  end

  Djinn.log_debug("zookeeper configuration=#{zoocfg}")
  File.open('/etc/zookeeper/conf/zoo.cfg', 'w+') { |file| file.write(zoocfg) }

  Djinn.log_debug("zookeeper myid=#{myid}")
  File.open('/etc/zookeeper/conf/myid', 'w+') { |file| file.write(myid) }

  # set max heap memory
  Djinn.log_run('sed -i s/^JAVA_OPTS=.*/JAVA_OPTS=\"-Xmx1024m\"/' \
                ' /etc/zookeeper/conf/environment')
end

def start_zookeeper(clear_datastore)
  Djinn.log_info('Starting zookeeper.')

  if clear_datastore
    Djinn.log_info('Removing old zookeeper state.')
    Djinn.log_run_sudo('rm -rfv /var/lib/zookeeper')
    Djinn.log_run_sudo("rm -rfv #{DATA_LOCATION}")
  end

  unless File.directory?(DATA_LOCATION.to_s)
    Djinn.log_info('Initializing ZooKeeper.')
    # Let's stop zookeeper in case it is still running.
    ServiceHelper.stop(ZOOKEEPER_SERVICE_NAME)

    # Let's create the new location for zookeeper.
    Djinn.log_run("mkdir -pv #{DATA_LOCATION}")
    Djinn.log_run_sudo("chown -Rv zookeeper:zookeeper #{DATA_LOCATION}")
  end

  # myid is needed for multi node configuration.
  Djinn.log_run_sudo("ln -sfv /etc/zookeeper/conf/myid #{DATA_LOCATION}/myid")

  ServiceHelper.start(ZOOKEEPER_SERVICE_NAME)
end

def is_zookeeper_running?
  output = ServiceHelper.is_running?(ZOOKEEPER_SERVICE_NAME)
  Djinn.log_debug("Checking if zookeeper is already running: #{output}")
  output
end

def stop_zookeeper
  Djinn.log_info('Stopping ZooKeeper')
  ServiceHelper.stop(ZOOKEEPER_SERVICE_NAME)
end
