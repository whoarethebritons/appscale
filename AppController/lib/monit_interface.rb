#!/usr/bin/ruby -w

# Imports within Ruby's standard libraries.
require 'monitor'
require 'tmpdir'

# Imports AppScale's libraries.
require 'helperfunctions'


# Where we save the configuration file.
MONIT_CONFIG = "/etc/monit/conf.d"


# Monit is finicky when it comes to multiple commands at the same time.
# Let's make sure we serialize access.
MONIT_LOCK = Monitor.new()


# Monit requires a bit of time after doing a reload to ensure no request
# is lost.
SMALL_WAIT = 2


# AppScale uses monit to start processes, restart them if they die, or kill and
# restart them if they take up too much CPU or memory. This module abstracts
# away interfacing with monit directly.
module MonitInterface

  
  # The location on the local filesystem of the monit executable.
  MONIT = "/usr/bin/monit"

  def self.start_monit()
    ret = system("sudo service --status-all 2> /dev/null | grep monit | grep + > /dev/null")
    self.run_cmd("service monit start") unless ret
    return ret
  end

  # Starts a basic service. The start_cmd should be designed to run in the
  # foreground, and it should not create its own pidfile.
  def self.start(watch, start_cmd, ports=nil, env_vars=nil, mem=nil)

    reload_monit = false
    ports = [nil] if ports.nil?
    ports.each { |port|
      # Convert symbol to string.
      process_name = "#{watch}"
      full_start_cmd = start_cmd
      unless port.nil?
        full_start_cmd += " -p #{port}"
        process_name += "-#{port}"
      end

      new_config = self.service_config(
        process_name, watch, full_start_cmd, env_vars, mem)

      monit_file = "#{MONIT_CONFIG}/appscale-#{process_name}.cfg"
      reload_required = self.update_config(monit_file, new_config)
      reload_monit = true if reload_required

      Djinn.log_info("Starting #{process_name} with command #{full_start_cmd}")
    }

    self.run_cmd('service monit reload', true) if reload_monit
    self.run_cmd("#{MONIT} start -g #{watch}")
  end

  # Starts a daemonized service. The start_cmd should be designed to start a
  # background process, and it should create its own pidfile.
  def self.start_daemon(watch, start_cmd, stop_cmd, pidfile)
    config = <<CONFIG
CHECK PROCESS #{watch} PIDFILE "#{pidfile}"
  group #{watch}
  start program = "#{start_cmd}"
  stop program = "#{stop_cmd}"
CONFIG

    monit_file = "#{MONIT_CONFIG}/appscale-#{watch}.cfg"
    reload_required = self.update_config(monit_file, config)
    self.run_cmd('service monit reload', true) if reload_required
    self.run_cmd("#{MONIT} start #{watch}")
  end

  # Starts a custom service. The start_cmd should be designed to start a
  # background process, and it should not create a pidfile.
  def self.start_custom(watch, start_cmd, stop_cmd, match_cmd)
    config = <<CONFIG
CHECK PROCESS #{watch} MATCHING "#{match_cmd}"
  group #{watch}
  start program = "#{start_cmd}"
  stop program = "#{stop_cmd}"
CONFIG

    monit_file = "#{MONIT_CONFIG}/appscale-#{watch}.cfg"
    reload_required = self.update_config(monit_file, config)
    self.run_cmd('service monit reload', true) if reload_required
    self.run_cmd("#{MONIT} start #{watch}")
  end

  def self.update_config(monit_file, config)
    begin
      reload_required = File.read(monit_file) != config
    rescue Errno::ENOENT
      reload_required = true
    end
    HelperFunctions.write_file(monit_file, config) if reload_required
    return reload_required
  end

  def self.restart(watch)
    self.run_cmd("#{MONIT} restart -g #{watch}")
  end

  # This function unmonitors and optionally stops the service, and removes
  # the monit configuration file.
  def self.stop(watch, stop=true)
    # To make sure the service is stopped, we query monit till the service
    # is not any longer running.
    running = true
    while running
      if stop
        Djinn.log_info("stop_monitoring: stopping service #{watch}.")
        self.run_cmd("#{MONIT} stop -g #{watch}")
      else
        Djinn.log_info("stop_monitoring: unmonitor service #{watch}.")
        self.run_cmd("#{MONIT} unmonitor -g #{watch}")
      end

      10.downto(0) {
        if not self.is_running?(watch)
          running = false
          break
        end
        Djinn.log_debug("Waiting for monit to stop #{watch}.")
        Kernel.sleep(SMALL_WAIT)
      }
    end

    # Now let's find the corresponding configuration file and remove it.
    config = Dir.glob("#{MONIT_CONFIG}/appscale-#{watch}*")
    if config.length > 1
      Djinn.log_info("Found multiple monit config matches for #{watch}: #{config}.")
    end
    FileUtils.rm_rf(config)
    self.run_cmd('service monit reload', true)
  end

  def self.service_config(process_name, group, start_cmd, env_vars, mem)

    # Monit doesn't support environment variables in its DSL, so if the caller
    # wants environment variables passed to the app, we have to collect them and
    # prepend it to the executable string.
    env_vars_str = ""
    if !env_vars.nil? and env_vars.length > 0
      env_vars.each { |key, value|
        env_vars_str += "#{key}=#{value} "
      }
    end

    # Use start-stop-daemon to handle pidfiles and start process in background.
    start_stop_daemon = `which start-stop-daemon`.chomp

    # Use bash to redirect the process's output to a log file.
    bash = `which bash`.chomp
    rm = `which rm`.chomp

    pidfile = "/var/run/appscale/#{process_name}.pid"
    logfile = "/var/log/appscale/#{process_name}.log"
    bash_exec = "exec env #{env_vars_str} #{start_cmd} >> #{logfile} 2>&1"

    start_args = ['--start',
                  '--background',
                  '--make-pidfile',
                  '--pidfile', pidfile,
                  '--startas', "#{bash} -- -c '#{bash_exec}'"]

    stop_cmd = "#{start_stop_daemon} --stop --pidfile #{pidfile} " +
               "--retry=TERM/20/KILL/5 && " +
               "#{rm} #{pidfile}"

    contents = <<BOO
CHECK PROCESS #{process_name} PIDFILE "#{pidfile}"
  group #{group}
  start program = "#{start_stop_daemon} #{start_args.join(' ')}"
  stop program = "#{bash} -c '#{stop_cmd}'"
BOO

    # If we have a valid 'mem' option, set the max memory for this
    # process.
    begin
      max_mem = Integer(mem)
      contents += "\n  if totalmem > #{max_mem} MB for 10 cycles then restart"
    rescue ArgumentError, TypeError
      # It was not an integer, ignoring it.
    end

    return contents
  end

  def self.is_running?(watch)
    output = self.run_cmd("#{MONIT} summary | grep #{watch} | grep -E \'(Running|Initializing)\'")
    return (not output == "")
  end

  # Checks if an AppServer instance is running.
  #
  # Args:
  #   version_key: A string specifying a version key.
  #   port: An integer specifying a port.
  # Returns:
  #   A boolean indicating whether or not the instance is running.
  def self.instance_running?(version_key, port)
    output = self.run_cmd("#{MONIT} summary")
    output.each_line { |entry|
      next unless entry.include?(version_key)
      next unless entry.include?(port.to_s)
      return entry.include?('Running') || entry.include?('Initialized')
    }
    return false
  end

  # This function returns a list of running applications: the
  # dev_appservers needs to still be monitored by monit.
  # Returns:
  #   A list of application:port records.
  def self.running_appengines()
    appengines = []
    output = self.run_cmd("#{MONIT} summary | grep -E \'app___.*(Running|Initializing)\'")
    appengines_raw = output.gsub! /Process 'app___(.*)-([0-9]*).*/, '\1:\2'
    if appengines_raw
      appengines_raw.split("\n").each{ |appengine|
        appengines << appengine if !appengine.split(":")[1].nil?
      }
    end

    Djinn.log_debug("Found these appservers processes running: #{appengines}.")
    return appengines
  end

  private
  def self.run_cmd(cmd, sleep=false)
    output = ""
    MONIT_LOCK.synchronize {
      output = Djinn.log_run_sudo(cmd)
      # Some command (ie reload) requires some extra time to ensure monit
      # is ready for the subsequent command.
      Kernel.sleep(SMALL_WAIT) if sleep
    }
    return output
  end
end
