#!/usr/bin/ruby -w

$:.unshift File.join(File.dirname(__FILE__))
require 'helperfunctions'
require 'monit_interface'

# Starts and stops the backup and recovery service.
module BackupRecoveryService
  # Starts the BR Service on this machine. We don't want to monitor
  # it ourselves, so just tell monit to start it and watch it.
  def self.start
    bk_service = scriptname
    start_cmd = bk_service.to_s
    MonitInterface.start(:backup_recovery_service, start_cmd)
  end

  # Stops the backup/recovery service running on this machine. Since it's
  # managed by monit, just tell monit to shut it down.
  def self.stop
    MonitInterface.stop(:backup_recovery_service)
  end

  def self.scriptname
    `which appscale-br-server`.chomp
  end
end
