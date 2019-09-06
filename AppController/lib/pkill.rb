require 'posixpsutil'

module PKill
  def self.terminate(pid)
    Process.kill('TERM', pid)
  end
end

if __FILE__ == $0
  if ARGV.length == 2 and ARGV[0] == "terminate"
    PKill.terminate(ARGV[1]) 
  end
end
