#!/usr/bin/ruby -w

require 'fileutils'
require 'monitor'

$:.unshift File.join(File.dirname(__FILE__))
require 'helperfunctions'

require 'rubygems'
require 'json'
require 'zookeeper'

# A class of exceptions that we throw whenever we perform a ZooKeeper
# operation that does not return successfully (but does not normally
# throw an exception).
class FailedZooKeeperOperationException < StandardError
end

# Indicates that the requested version node was not found.
class VersionNotFound < StandardError
end

# The AppController employs the open source software ZooKeeper as a highly
# available naming service, to store and retrieve information about the status
# of applications hosted within AppScale. This class provides methods to
# communicate with ZooKeeper, and automates commonly performed functions by the
# AppController.
class ZKInterface
  # The port that ZooKeeper runs on in AppScale deployments.
  SERVER_PORT = 2181

  EPHEMERAL = true

  NOT_EPHEMERAL = false

  # The location in ZooKeeper where AppControllers can read and write
  # data to.
  APPCONTROLLER_PATH = '/appcontroller'.freeze

  # The location in ZooKeeper where the Shadow node will back up its state to,
  # and where other nodes will recover that state from.
  APPCONTROLLER_STATE_PATH = "#{APPCONTROLLER_PATH}/state".freeze

  # The location in ZooKeeper where the AppServer nodes will back up information
  # about each AppServer they host (e.g., the nginx, haproxy, and dev_appserver
  # ports that each AppServer binds to).
  APPSERVER_STATE_PATH = "#{APPCONTROLLER_PATH}/appservers".freeze

  # The location in ZooKeeper that contains a list of the IP addresses that
  # are currently running within AppScale.
  IP_LIST = "#{APPCONTROLLER_PATH}/ips".freeze

  # The location in ZooKeeper that AppControllers write information about their
  # node to, so that others can poll to see if they are alive and what roles
  # they've taken on.
  APPCONTROLLER_NODE_PATH = "#{APPCONTROLLER_PATH}/nodes".freeze

  # The location in ZooKeeper that nodes will try to acquire an ephemeral node
  # for, to use as a lock.
  APPCONTROLLER_LOCK_PATH = "#{APPCONTROLLER_PATH}/lock".freeze

  # The location in ZooKeeper that AppControllers write information about
  # which Google App Engine apps require additional (or fewer) AppServers to
  # handle the amount of traffic they are receiving.
  SCALING_DECISION_PATH = "#{APPCONTROLLER_PATH}/scale".freeze

  # The name of the file that nodes use to store the list of Google App Engine
  # instances that the given node runs.
  APP_INSTANCE = 'app_instance'.freeze

  ROOT_APP_PATH = '/apps'.freeze

  # The contents of files in ZooKeeper whose contents we don't care about
  # (e.g., where we care that's an ephemeral file or needed just to provide
  # a hierarchical filesystem-like interface).
  DUMMY_DATA = ''.freeze

  # The amount of time that has to elapse before Zookeeper expires the
  # session (and all ephemeral locks) with our client. Setting this value at
  # or below 10 seconds has historically not been a good idea for us (as
  # sessions repeatedly time out).
  TIMEOUT = 60

  # Initializes a new ZooKeeper connection to the IP address specified.
  # Callers should use this when they know exactly which node hosts ZooKeeper.
  def self.init_to_ip(client_ip, ip)
    Djinn.log_debug("Waiting for #{ip}:#{SERVER_PORT} to open")
    HelperFunctions.sleep_until_port_is_open(ip, SERVER_PORT)

    @@client_ip = client_ip
    @@ip = ip

    @@lock = Monitor.new unless defined?(@@lock)
    @@lock.synchronize {
      if defined?(@@zk)
        Djinn.log_debug('Closing old connection to zookeeper.')
        @@zk.close!
      end
      Djinn.log_debug("Opening connection to zookeeper at #{ip}.")
      @@zk = Zookeeper.new("#{ip}:#{SERVER_PORT}", TIMEOUT)
    }
  end

  # This method check if we are already connected to a zookeeper server.
  #
  # Returns:
  #   A boolean to indicate if we are already connected to a zookeeper
  #   server.
  def self.is_connected?
    ret = false
    ret = @@zk.connected? if defined?(@@zk)
    Djinn.log_debug("Connection status with zookeeper server: #{ret}.")
    ret
  end

  # Creates a new connection to use with ZooKeeper. Useful for scenarios
  # where the ZooKeeper library has terminated our connection but we still
  # need it. Also recreates any ephemeral links that were lost when the
  # connection was disconnected.
  def self.reinitialize
    init_to_ip(@@client_ip, @@ip)
    set_live_node_ephemeral_link(@@client_ip)
  end

  def self.add_revision_entry(revision_key, ip, md5)
    revision_path = "#{ROOT_APP_PATH}/#{revision_key}/#{ip}"
    set(revision_path, md5, NOT_EPHEMERAL)
  end

  def self.remove_revision_entry(revision_key, ip)
    delete("#{ROOT_APP_PATH}/#{revision_key}/#{ip}")
  end

  def self.get_revision_hosters(revision_key, keyname)
    revision_hosters = get_children("#{ROOT_APP_PATH}/#{revision_key}")
    converted = []
    revision_hosters.each { |host|
      converted << DjinnJobData.new(get_job_data_for_ip(host), keyname)
    }
    converted
  end

  def self.get_revision_md5(revision_key, ip)
    get("#{ROOT_APP_PATH}/#{revision_key}/#{ip}").chomp
  end

  def self.get_appcontroller_state
    JSON.load(get(APPCONTROLLER_STATE_PATH))
  end

  def self.write_appcontroller_state(state)
    # Create the top-level AC dir, then the actual node that stores
    # our data
    set(APPCONTROLLER_PATH, DUMMY_DATA, NOT_EPHEMERAL)
    set(APPCONTROLLER_STATE_PATH, JSON.dump(state), NOT_EPHEMERAL)
  end

  # Gets a lock that AppControllers can use to have exclusive write access
  # (between other AppControllers) to the ZooKeeper hierarchy located at
  # APPCONTROLLER_PATH. It returns a boolean that indicates whether or not
  # it was able to acquire the lock or not.
  def self.get_appcontroller_lock
    unless exists?(APPCONTROLLER_PATH)
      set(APPCONTROLLER_PATH, DUMMY_DATA, NOT_EPHEMERAL)
    end

    info = run_zookeeper_operation {
      @@zk.create(path: APPCONTROLLER_LOCK_PATH, ephemeral: EPHEMERAL,
                  data: @@client_ip)
    }
    return true if info[:rc].zero?

    Djinn.log_warn("Couldn't get the AppController lock, saw info " \
                   "#{info.inspect}")
    false
  end

  # Releases the lock that AppControllers use to have exclusive write access,
  # which was acquired via self.get_appcontroller_lock().
  def self.release_appcontroller_lock
    delete(APPCONTROLLER_LOCK_PATH)
  end

  # This method provides callers with an easier way to read and write to
  # AppController data in ZooKeeper. This is useful for methods that aren't
  # sure if they already have the ZooKeeper lock or not, but definitely need
  # it and don't want to accidentally cause a deadlock (grabbing the lock when
  # they already have it).
  def self.lock_and_run(&block)
    # Create the ZK lock path if it doesn't exist.
    unless exists?(APPCONTROLLER_PATH)
      set(APPCONTROLLER_PATH, DUMMY_DATA, NOT_EPHEMERAL)
    end

    # Try to get the lock, and if we can't get it, see if we already have
    # it. If we do, move on (but don't release it later since this block
    # didn't grab it), and if we don't have it, try again.
    got_lock = false
    begin
      if get_appcontroller_lock
        got_lock = true
      else # it may be that we already have the lock
        info = run_zookeeper_operation {
          @@zk.get(path: APPCONTROLLER_LOCK_PATH)
        }
        owner = JSON.load(info[:data])
        if @@client_ip == owner
          got_lock = false
        else
          raise "Tried to get the lock, but it's currently owned by #{owner}."
        end
      end
    rescue => e
      sleep_time = 5
      Djinn.log_warn("Saw #{e.inspect}. Retrying in #{sleep_time} seconds.")
      Kernel.sleep(sleep_time)
      retry
    end

    begin
      yield  # invoke the user's block, and catch any uncaught exceptions
    rescue => except
      Djinn.log_error("Ran caller's block but saw an Exception of class " \
        "#{except.class}")
      raise except
    ensure
      release_appcontroller_lock if got_lock
    end
  end

  # Returns a Hash containing the list of the IPs that are currently running
  # within AppScale as well as a timestamp corresponding to the time when the
  # latest node updated this information.
  def self.get_ip_info
    JSON.load(get(IP_LIST))
  end

  # Add the given IP to the list of IPs that we store in ZooKeeper. If the IPs
  # file doesn't exist in ZooKeeper, create it and add in the given IP address.
  # We also update the timestamp associated with this list so that others know
  # to update it as needed.
  def self.add_ip_to_ip_list(ip)
    new_timestamp = Time.now.to_i

    if exists?(IP_LIST)
      # See if our IP is in the list of IPs that are up, and if not,
      # append it to the list and update the timestamp so that everyone
      # else will update their local copies.
      data = JSON.load(get(IP_LIST))
      if !data['ips'].include?(ip)
        Djinn.log_debug('IPs file does not include our IP - adding it in')
        data['ips'] << ip
        data['last_updated'] = new_timestamp
        set(IP_LIST, JSON.dump(data), NOT_EPHEMERAL)
        Djinn.log_debug('Updated timestamp in ips list to ' \
          "#{data['last_updated']}")
      else
        Djinn.log_debug('IPs file already includes our IP - skipping')
      end
    else
      Djinn.log_debug('IPs file does not exist - creating it')
      data = { 'ips' => [ip], 'last_updated' => new_timestamp }
      set(IP_LIST, JSON.dump(data), NOT_EPHEMERAL)
      Djinn.log_debug('Updated timestamp in ips list to ' \
        "#{data['last_updated']}")
    end

    new_timestamp
  end

  # Accesses the list of IP addresses stored in ZooKeeper and removes the
  # given IP address from that list.
  def self.remove_ip_from_ip_list(ip)
    return unless exists?(IP_LIST)

    data = JSON.load(get(IP_LIST))
    data['ips'].delete(ip)
    new_timestamp = Time.now.to_i
    data['last_updated'] = new_timestamp
    set(IP_LIST, JSON.dump(data), NOT_EPHEMERAL)
    new_timestamp
  end

  # Updates the timestamp in the IP_LIST file, to let other nodes know that
  # an update has been made and that they should update their local @nodes
  def self.update_ips_timestamp
    data = JSON.load(get(IP_LIST))
    new_timestamp = Time.now.to_i
    data['last_updated'] = new_timestamp
    set(IP_LIST, JSON.dump(data), NOT_EPHEMERAL)
    Djinn.log_debug("Updated timestamp in ips list to #{data['last_updated']}")
    new_timestamp
  end

  # Queries ZooKeeper for a list of all IPs that are currently up, and then
  # checks if each of those IPs has an ephemeral link indicating that they
  # are alive. Returns an Array of IPs corresponding to failed nodes.
  def self.get_failed_nodes
    failed_nodes = []

    ips = get_ip_info['ips']
    Djinn.log_debug("All IPs are [#{ips.join(', ')}]")

    ips.each { |ip|
      if exists?("#{APPCONTROLLER_NODE_PATH}/#{ip}/live")
        Djinn.log_debug("Node at #{ip} is alive")
      else
        Djinn.log_debug("Node at #{ip} has failed")
        failed_nodes << ip
      end
    }

    Djinn.log_debug("Failed nodes are [#{failed_nodes.join(', ')}]")
    failed_nodes
  end

  # Creates files in ZooKeeper that relate to a given AppController's
  # role information, so that other AppControllers can detect if it has
  # failed, and if so, what functionality it was providing at the time.
  def self.write_node_information(node, done_loading)
    # Create the folder for all nodes if it doesn't exist.
    unless exists?(APPCONTROLLER_NODE_PATH)
      run_zookeeper_operation {
        @@zk.create(path: APPCONTROLLER_NODE_PATH,
                    ephemeral: NOT_EPHEMERAL, data: DUMMY_DATA)
      }
    end

    # Create the folder for this node.
    my_ip_path = "#{APPCONTROLLER_NODE_PATH}/#{node.private_ip}"
    run_zookeeper_operation {
      @@zk.create(path: my_ip_path, ephemeral: NOT_EPHEMERAL,
                  data: DUMMY_DATA)
    }

    # Create an ephemeral link associated with this node, which other
    # AppControllers can use to quickly detect dead nodes.
    set_live_node_ephemeral_link(node.private_ip)

    # Since we're reporting on the roles we've started, we are done loading
    # roles right now, so write that information for others to read and act on.
    set_done_loading(node.private_ip, done_loading)

    # Finally, dump the data from this node to ZK, so that other nodes can
    # reconstruct it as needed.
    set_job_data_for_ip(node.private_ip, node.to_hash)
  end

  # Deletes all information for a given node, whose data is stored in ZooKeeper.
  def self.remove_node_information(ip)
    recursive_delete("#{APPCONTROLLER_NODE_PATH}/#{ip}")
  end

  # Checks ZooKeeper to see if the given node has finished loading its roles,
  # which it indicates via a file in a particular path.
  def self.is_node_done_loading?(ip)
    return false unless exists?(APPCONTROLLER_NODE_PATH)

    loading_file = "#{APPCONTROLLER_NODE_PATH}/#{ip}/done_loading"
    return false unless exists?(loading_file)

    begin
      contents = get(loading_file)
      return contents == 'true'
    rescue FailedZooKeeperOperationException
      return false
    end
  end

  # Writes the ephemeral link in ZooKeeper that represents a given node
  # being alive. Callers should only use this method to indicate that their
  # own node is alive, and not do it on behalf of other nodes.
  def self.set_live_node_ephemeral_link(ip)
    run_zookeeper_operation {
      @@zk.create(path: "#{APPCONTROLLER_NODE_PATH}/#{ip}/live",
                  ephemeral: EPHEMERAL, data: DUMMY_DATA)
    }
  end

  # Provides a convenience function that callers can use to indicate that their
  # node is done loading (if they have finished starting/stopping roles), or is
  # not done loading (if they have roles they need to start or stop).
  def self.set_done_loading(ip, val)
    zk_value = val ? 'true' : 'false'
    set("#{APPCONTROLLER_NODE_PATH}/#{ip}/done_loading",
        zk_value, NOT_EPHEMERAL)
  end

  # Checks ZooKeeper to see if the given node is alive, by checking if the
  # ephemeral file it has created is still present.
  def self.is_node_live?(ip)
    exists?("#{APPCONTROLLER_NODE_PATH}/#{ip}/live")
  end

  def self.get_job_data_for_ip(ip)
    JSON.load(get("#{APPCONTROLLER_NODE_PATH}/#{ip}/job_data"))
  end

  def self.set_job_data_for_ip(ip, job_data)
    set("#{APPCONTROLLER_NODE_PATH}/#{ip}/job_data",
        JSON.dump(job_data), NOT_EPHEMERAL)
  end

  def self.get_versions
    active_versions = []
    get_children('/appscale/projects').each { |project_id|
      services_node = "/appscale/projects/#{project_id}/services"
      get_children(services_node).each { |service_id|
        versions_node = "/appscale/projects/#{project_id}/services/" \
          "#{service_id}/versions"
        get_children(versions_node).each { |version_id|
          active_versions << [project_id, service_id,
                              version_id].join(Djinn::VERSION_PATH_SEPARATOR)
        }
      }
    }
    active_versions
  end

  def self.get_version_details(project_id, service_id, version_id)
    version_node = "/appscale/projects/#{project_id}/services/#{service_id}" \
      "/versions/#{version_id}"
    begin
      version_details_json = get(version_node)
    rescue FailedZooKeeperOperationException
      raise VersionNotFound,
            "#{project_id}/#{service_id}/#{version_id} does not exist"
    end
    JSON.load(version_details_json)
  end

  # Defines deployment-wide defaults for runtime parameters.
  def self.set_runtime_params(parameters)
    runtime_params_node = '/appscale/config/runtime_parameters'
    ensure_path('/appscale/config')
    set(runtime_params_node, JSON.dump(parameters), false)
  end

  # Writes new configs for node stats profiling to zookeeper.
  def self.update_hermes_nodes_profiling_conf(is_enabled, interval)
    configs_node = '/appscale/stats/profiling/nodes'
    ensure_path(configs_node)
    configs = {
      'enabled' => is_enabled,
      'interval' => interval
    }
    set(configs_node, JSON.dump(configs), NOT_EPHEMERAL)
  end

  # Writes new configs for processes stats profiling to zookeeper
  def self.update_hermes_processes_profiling_conf(is_enabled, interval, is_detailed)
    configs_node = '/appscale/stats/profiling/processes'
    ensure_path(configs_node)
    configs = {
      'enabled' => is_enabled,
      'interval' => interval,
      'detailed' => is_detailed
    }
    set(configs_node, JSON.dump(configs), NOT_EPHEMERAL)
  end

  # Writes new configs for proxies stats profiling to zookeeper
  def self.update_hermes_proxies_profiling_conf(is_enabled, interval, is_detailed)
    configs_node = '/appscale/stats/profiling/proxies'
    ensure_path(configs_node)
    configs = {
      'enabled' => is_enabled,
      'interval' => interval,
      'detailed' => is_detailed
    }
    set(configs_node, JSON.dump(configs), NOT_EPHEMERAL)
  end

  def self.run_zookeeper_operation(&block)
    begin
      yield
    rescue ZookeeperExceptions::ZookeeperException::ConnectionClosed,
      ZookeeperExceptions::ZookeeperException::NotConnected,
      ZookeeperExceptions::ZookeeperException::SessionExpired

      Djinn.log_warn('Lost our ZooKeeper connection - making a new ' \
        'connection and trying again.')
      reinitialize
      Kernel.sleep(1)
      retry
    rescue => e
      backtrace = e.backtrace.join("\n")
      Djinn.log_warn("Saw a transient ZooKeeper error: #{e}\n#{backtrace}")
      Kernel.sleep(1)
      retry
    end
  end

  def self.exists?(key)
    unless defined?(@@zk)
      raise FailedZooKeeperOperationException.new('ZKinterface has not ' \
        'been initialized yet.')
    end

    self.run_zookeeper_operation {
      @@zk.get(path: key)[:stat].exists
    }
  end

  def self.get(key)
    unless defined?(@@zk)
      raise FailedZooKeeperOperationException.new('ZKinterface has not ' \
        'been initialized yet.')
    end

    info = run_zookeeper_operation {
      @@zk.get(path: key)
    }
    if info[:rc].zero?
      return info[:data]
    else
      raise FailedZooKeeperOperationException.new("Failed to get #{key}, " \
        "with info #{info.inspect}")
    end
  end

  def self.get_children(key)
    unless defined?(@@zk)
      raise FailedZooKeeperOperationException.new('ZKinterface has not ' \
        'been initialized yet.')
    end

    children = run_zookeeper_operation {
      @@zk.get_children(:path => key)[:children]
    }

    if children.nil?
      return []
    else
      return children
    end
  end

  # Recursively create a path if it doesn't exist.
  def self.ensure_path(path)
    # Remove preceding slash.
    nodes = path.split('/')[1..-1]
    i = 0
    while i < nodes.length
      node = '/' + nodes[0..i].join('/')
      run_zookeeper_operation {
        @@zk.create(path: node)
      }
      i += 1
    end
  end

  def self.set(key, val, ephemeral)
    unless defined?(@@zk)
      raise FailedZooKeeperOperationException.new('ZKinterface has not ' \
        'been initialized yet.')
    end

    retries_left = 5
    begin
      info = {}
      if exists?(key)
        info = run_zookeeper_operation {
          @@zk.set(path: key, data: val)
        }
      else
        info = run_zookeeper_operation {
          @@zk.create(path: key, ephemeral: ephemeral, data: val)
        }
      end

      unless info[:rc].zero?
        raise FailedZooKeeperOperationException.new('Failed to set path ' \
          "#{key} with data #{val}, ephemeral = #{ephemeral}, saw " \
          "info #{info.inspect}")
      end
    rescue FailedZooKeeperOperationException => e
      retries_left -= 1
      Djinn.log_warn('Saw a failure trying to write to ZK, with ' \
        "info [#{e}]")
      if retries_left > 0
        Djinn.log_warn("Retrying write operation, with #{retries_left}" \
          ' retries left')
        Kernel.sleep(5)
        retry
      else
        Djinn.log_error('[ERROR] Failed to write to ZK and no retries ' \
          'left. Skipping on this write for now.')
      end
    end
  end

  def self.recursive_delete(key)
    child_info = get_children(key)
    return if child_info.empty?

    child_info.each { |child|
      recursive_delete("#{key}/#{child}")
    }

    begin
      delete(key)
    rescue FailedZooKeeperOperationException
      Djinn.log_error("Failed to delete key #{key} - continuing onward")
    end
  end

  def self.delete(key)
    unless defined?(@@zk)
      raise FailedZooKeeperOperationException.new('ZKinterface has not ' \
        'been initialized yet.')
    end

    info = run_zookeeper_operation {
      @@zk.delete(path: key)
    }
    unless info[:rc].zero?
      Djinn.log_error("Delete failed - #{info.inspect}")
      raise FailedZooKeeperOperationException.new('Failed to delete ' \
        " path #{key}, saw info #{info.inspect}")
    end
  end
end
