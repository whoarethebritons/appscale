#!/usr/bin/ruby -w

require 'base64'
require 'openssl'
require 'soap/rpc/driver'
require 'timeout'
require 'helperfunctions'

require 'rubygems'
require 'json'

# Number of seconds to wait before timing out when doing a SOAP call.
# This number should be higher than the maximum time required for remote calls
# to properly execute (i.e., starting a process may take more than 2 minutes).
MAX_TIME_OUT = 180

# This is transitional glue code as we shift from ruby to python. The 
# AppManager is written in python and hence we use a SOAP client to communicate
# between the two services.
class AppManagerClient

  # The connection to use and IP to connect to
  attr_reader :conn, :ip

  # The port that the AppManager binds to, by default.
  SERVER_PORT = 17445

  # Initialization function for AppManagerClient
  def initialize(ip)
    @ip = ip

    @conn = SOAP::RPC::Driver.new("http://#{@ip}:#{SERVER_PORT}")
    @conn.options["protocol.http.connect_timeout"] = MAX_TIME_OUT
    @conn.options["protocol.http.send_timeout"] = MAX_TIME_OUT
    @conn.options["protocol.http.receive_timeout"] = MAX_TIME_OUT
    @conn.add_method("start_app", "config")
    @conn.add_method("stop_app", "app_name")
    @conn.add_method("stop_app_instance", "app_name", "port")
  end

  # Check the comments in AppController/lib/app_controller_client.rb.
  def make_call(time, retry_on_except, callr)
    begin
      Timeout.timeout(time) {
        begin
          yield if block_given?
        rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH,
          OpenSSL::SSL::SSLError, NotImplementedError, Errno::EPIPE,
          Errno::ECONNRESET, SOAP::EmptyResponseError, StandardError => e
          if retry_on_except
            Kernel.sleep(1)
            Djinn.log_debug("[#{callr}] exception in make_call to " +
              "#{@ip}:#{SERVER_PORT}. Exception class: #{e.class}. Retrying...")
            retry
          else
            trace = e.backtrace.join("\n")
            Djinn.log_warn("Exception encountered while talking to " +
              "#{@ip}:#{SERVER_PORT}.\n#{trace}")
            raise FailedNodeException.new("Exception #{e.class}:#{e.message} encountered " +
              "while talking to #{@ip}:#{SERVER_PORT}.")
          end
        end
      }
    rescue Timeout::Error
      Djinn.log_warn("[#{callr}] SOAP call to #{@ip} timed out")
      raise FailedNodeException.new("Time out talking to #{@ip}:#{SERVER_PORT}")
    end
  end

  # Wrapper for SOAP call to the AppManager to start an process instance of
  # an application server.
  #
  # Args:
  #   app_name: Name of the application
  #   app_port: The port to run the application server
  #   login_ip: The public IP of this deployemnt
  #   load_balancer_port: The port of the load balancer
  #   language: The language the application is written in
  #   env_vars: A Hash of environemnt variables that should be passed to the
  #     application to start.
  #   max_memory: An Integer that names the maximum amount of memory (in
  #     megabytes) that should be used for this App Engine app.
  #   syslog_server: The IP address of the remote syslog server to use.
  # Returns:
  #   The PID of the process started
  # Note:
  #   We currently send hashes over in SOAP using json because
  #   of incompatibilities between SOAP mappings from ruby to python.
  #   As we convert over to python we should use native dictionaries.
  #
  def start_app(app_name,
                app_port,
                login_ip,
                language,
                env_vars,
                max_memory=500,
                syslog_server="")
    config = {'app_name' => app_name,
              'app_port' => app_port,
              'login_ip' => login_ip,
              'language' => language,
              'env_vars' => env_vars,
              'max_memory' => max_memory,
              'syslog_server' => syslog_server}
    json_config = JSON.dump(config)
    result = -1
    make_call(MAX_TIME_OUT, false, "start_app") {
      result = @conn.start_app(json_config)
    }
    return Integer(result)
  end

  # Wrapper for SOAP call to the AppManager to stop an application
  # process instance from the current host.
  #
  # Args:
  #   app_name: The name of the application
  #   port: The port the process instance of the application is running
  # Returns:
  #   True on success, False otherwise
  #
  def stop_app_instance(app_name, port)
    result = ""
    make_call(MAX_TIME_OUT, false, "stop_app_instance") {
      result = @conn.stop_app_instance(app_name, port)
    }
    return result
  end

  # Wrapper for SOAP call to the AppManager to remove an application
  # from the current host.
  # 
  # Args:
  #   app_name: The name of the application
  # Returns:
  #   True on success, False otherwise
  #
  def stop_app(app_name)
    result = ""
    make_call(MAX_TIME_OUT, false, "stop_app") {
      result = @conn.stop_app(app_name)
    }
    return result
  end
end
