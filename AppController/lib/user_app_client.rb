#!/usr/bin/ruby -w

# Imports within Ruby's standard libraries
require 'base64'
require 'openssl'
require 'soap/rpc/driver'
require 'timeout'

require 'helperfunctions'


# AppControllers and other services need to read or write data relating to users
# and applications hosted in AppScale. Since it has to be done in a
# database-agnostic fashion, we throw up a special server that responds to SOAP
# requests for this information, called the UserAppServer. This class provides
# convenience methods for interacting with the UserAppServer.
class UserAppClient
  attr_reader :conn, :ip, :secret

  # The default name for the server.
  NAME = "UserAppServer"

  # The port that the UserAppServer binds to, by default.
  SSL_SERVER_PORT = 4343

  # The port the server is listening to.
  SERVER_PORT = 4342

  # The port used to have HAProxy in front of the UserAppServer.
  HAPROXY_SERVER_PORT = 4341

  # This is the minimum Timeout to use when talking to the datastore.
  DS_MIN_TIMEOUT = 20


  def initialize(ip, secret)
    @ip = ip
    @secret = secret

    @conn = SOAP::RPC::Driver.new("https://#{@ip}:#{SSL_SERVER_PORT}")
    @conn.options["protocol.http.ssl_config.verify_mode"] = nil
    @conn.add_method("change_password", "user", "password", "secret")
    @conn.add_method("commit_new_user", "user", "passwd", "utype", "secret")
    @conn.add_method("commit_new_app", "user", "appname", "language", "secret")
    @conn.add_method("commit_tar", "app_name", "tar", "secret")
    @conn.add_method("delete_app", "appname", "secret")
    @conn.add_method("does_app_exist", "appname", "secret")
    @conn.add_method("enable_app", "appname", "secret")
    @conn.add_method("is_app_enabled", "appname", "secret")
    @conn.add_method("is_user_cloud_admin", "username", "secret")
    @conn.add_method("does_user_exist", "username", "secret")
    @conn.add_method("get_user_data", "username", "secret")
    @conn.add_method("get_app_data", "appname", "secret")
    @conn.add_method("delete_instance", "appname", "host", "port", "secret")
    @conn.add_method("get_tar", "app_name", "secret")
    @conn.add_method("add_instance", "appname", "host", "port", "https_port", "secret")
    @conn.add_method("get_all_apps", "secret")
    @conn.add_method("get_all_users", "secret")
    @conn.add_method("set_cloud_admin_status", "username", "is_cloud_admin", "secret")
    @conn.add_method("set_capabilities", "username", "capabilities", "secret")
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


  def commit_new_user(user, encrypted_password, user_type, retry_on_except=true)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "commit_new_user") {
      result = @conn.commit_new_user(user, encrypted_password, user_type, @secret)
    }

    if result == "true"
      puts "\nYour user account has been created successfully."
    elsif result == "false"
      HelperFunctions.log_and_crash("\nWe were unable to create your user " +
        "account. Please contact your cloud administrator for further details.")
    else
      puts "\n[unexpected] Commit new user returned: [#{result}]"
    end
    return result
  end

  def commit_new_app(user, app_name, language, file_location)
    commit_new_app_name(user, app_name, language)
    commit_tar(app_name, file_location)
  end

  def commit_new_app_name(user, app_name, language, retry_on_except=true)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "commit_new_app_name") {
      result = @conn.commit_new_app(user, app_name, language, @secret)
    }

    if result == "true"
      puts "We have reserved the name #{app_name} for your application."
    elsif result == "Error: appname already exist"
      puts "We are uploading a new version of the application #{app_name}."
    elsif result == "Error: User not found"
      HelperFunctions.log_and_crash("We were unable to reserve the name of " +
        "your application. Please contact your cloud administrator for more " +
        "information.")
    else
      puts "[unexpected] Commit new app says: [#{result}]"
    end
    return result
  end

  def commit_tar(app_name, file_location, retry_on_except=true)
    file = File.open(file_location, "rb")
    tar_contents = Base64.encode64(file.read)

    result = ""
    make_call(DS_MIN_TIMEOUT * 25, retry_on_except, "commit_tar") {
      result = @conn.commit_tar(app_name, tar_contents, @secret)
    }

    if result == "true"
      puts "#{app_name} was uploaded successfully."
    elsif result == "Error: app does not exist"
      HelperFunctions.log_and_crash("We were unable to upload your " +
        "application. Please contact your cloud administrator for more " +
        "information.")
    else
      puts "[unexpected] Commit new tar says: [#{result}]"
    end
  end

  def change_password(user, new_password, retry_on_except=true)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "change_password") {
      result = @conn.change_password(user, new_password, @secret)
    }

    if result == "true"
      puts "We successfully changed the password for the given user."
    elsif result == "Error: user not found"
      puts "We were unable to locate a user with the given username."
    else
      puts "[unexpected] Got this message back: [#{result}]"
    end
    return result
  end

  def delete_app(app, retry_on_except=true)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "delete_app") {
      result = @conn.delete_app(app, @secret)
    }

    if result == "true"
      return true
    else
      return result
    end
  end

  def does_app_exist?(app, retry_on_except=true)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "does_app_exist") {
      result = @conn.does_app_exist(app, @secret)
    }

    if result == "true"
      return true
    else
      return false
    end
  end

  def is_app_enabled?(app, retry_on_except=true)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "is_app_enabled") {
      result = @conn.is_app_enabled(app, @secret)
    }

    if result == "true"
      return true
    else
      return false
    end
  end

  def does_user_exist?(user, retry_on_except=true)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "does_user_exist") {
      result = @conn.does_user_exist(user, @secret)
    }

    return result
  end

  def enable_app(app, retry_on_except=true)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "enable_app") {
      result = @conn.enable_app(app, @secret)
    }

    if result == "true"
      return true
    else
      return result
    end
  end

  def get_user_data(username, retry_on_except=true)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "get_user_data") {
      result = @conn.get_user_data(username, @secret)
    }

    return result
  end

  def get_app_data(appname, retry_on_except=true)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "get_app_data") {
      result = @conn.get_app_data(appname, @secret)
    }
    if result[0..4] == "Error"
      msg = "get_app_data: failed to get data for app #{appname}."
      Djinn.log_debug(msg)
      raise FailedNodeException.new(msg)
    end

    return result
  end

  def delete_instance(appname, host, port, retry_on_except=true)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "delete_instance") {
      result = @conn.delete_instance(appname, host, port, @secret)
    }

    return result
  end

  def get_all_apps(retry_on_except=true)
    all_apps = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "get_all_apps") {
      all_apps = @conn.get_all_apps(@secret)
    }

    app_list = all_apps.split(":")
    if app_list[0] == "Error"
      raise FailedNodeException.new("get_all_apps: got #{all_apps}.")
    end
    app_list = app_list - [app_list[0]] # first item is a dummy value
    return app_list
  end

  def get_all_users(retry_on_except=true)
    all_users = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "get_all_users") {
      all_users = @conn.get_all_users(@secret)
    }

    user_list = all_users.split(":")
    if user_list[0] = "Error"
      raise FailedNodeException.new("get_all_users: got #{all_users}.")
    end
    user_list = user_list - [user_list[0]]  # first item is a dummy value
    return user_list
  end

  def get_tar(appname, retry_on_except=true)
    result = ""
    make_call(DS_MIN_TIMEOUT * 25, retry_on_except, "get_tar") {
      result = @conn.get_tar(appname, @secret)
    }

    return result
  end

  def add_instance(appname, host, port, https_port, retry_on_except=true)
    result = ""
    begin
      make_call(DS_MIN_TIMEOUT, retry_on_except, "add_instance") {
        result = @conn.add_instance(appname, host, port, https_port, @secret)
      }
    rescue FailedNodeException
      Djinn.log_error("Exception talking to the UserAppServer. " +
        "#{appname} may not have been updated.")
      return false
    end

    if result == "true"
      return true
    else
      return false
    end
  end

  def is_user_cloud_admin?(user, retry_on_except=true)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "is_user_cloud_admin") {
      result = @conn.is_user_cloud_admin(user, @secret)
    }

    if result == "true"
      return true
    else
      return false
    end
  end

  # This method finds the first user who is a cloud administrator. Since the
  # UserAppServer doesn't provide a function that does this for us, we have
  # to get a list of all the users that exist and individually query the
  # UAServer to find out who the cloud admin is.
  # TODO: Maybe cache who the cloud admin is to speed up future queries?
  def get_cloud_admin()
    user_list = get_all_users()

    user_list.each { |user|
      return user if is_user_cloud_admin?(user)
    }

    raise Exception.new("Couldn't find a cloud administrator")
  end

  def set_admin_role(username, is_cloud_admin, capabilities, retry_on_except=true)
    result_cloud_admin_status = set_cloud_admin_status(username, is_cloud_admin, retry_on_except)
    result_set_capabilities = set_capabilities(username, capabilities, retry_on_except)
    if result_cloud_admin_status and result_set_capabilities == "true"
      puts "We successfully set admin role for the given user."
      return "true"
    else
      puts "Got this message back while setting cloud admin status and capabilities:" +
        "Set cloud admin status: [#{result_cloud_admin_status}]" +
        "Set capabilities: [#{result_set_capabilities}]"
    end
  end

  def set_cloud_admin_status(username, is_cloud_admin, retry_on_except)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "set_cloud_admin_status") {
      result = @conn.set_cloud_admin_status(username, is_cloud_admin, @secret)
    }
    if result == "true"
      puts "We successfully set cloud admin status for the given user."
    elsif result == "Error: user not found"
      puts "We were unable to locate a user with the given username."
    else
      puts "[unexpected] Got this message back: [#{result}]"
    end
    return result
  end

  def set_capabilities(username, capabilities, retry_on_except)
    result = ""
    make_call(DS_MIN_TIMEOUT, retry_on_except, "set_capabilities") {
      result = @conn.set_capabilities(username, capabilities, @secret)
    }
    if result == "true"
      puts "We successfully set admin capabilities for the given user."
    elsif result == "Error: user not found"
      puts "We were unable to locate a user with the given username."
    else
      puts "[unexpected] Got this message back: [#{result}]"
    end
    return result
  end

end
