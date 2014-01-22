Gem::Specification.new do |s|
  s.name        = 's3_wrapper'
  s.version     = '0.0.1'
  s.date        = '2014-01-22'
  s.summary     = 'A tool for basic manipulation of files in S3'
  s.description = <<-EOS
    The intent of this gem is to simplify interactions with S3, and allow for
    more powerful searching and deleting conditions than provided by default in
    the AWS Ruby SDK or command line tools.
  EOS
  s.authors     = ['Dean Morin']
  s.email       = 'morin.dean@gmail.com'
  s.homepage    = 'http://github.com/deanmorin/s3_wrapper'
  s.license     = 'Unlicense'
  s.files       = ['lib/s3_wrapper.rb']
end
