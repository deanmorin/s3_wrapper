# encoding=utf-8
require 'aws-sdk'


class S3Wrapper

  def initialize(bucket, options={})
    @bucket = AWS::S3.new(options).buckets[bucket]
    @objects = @bucket.objects
  end

  def get(key)
    @objects[key].read
  end

  def get_file(key, filename=key)
    File.open(filename, 'w') do |fh|
      @objects[key].read do |chunk|
        fh.write(chunk)
      end
    end
  end

  def put(key, data)
    @objects[key].write(data)
  end

  def put_file(filename, key=nil)
    key ||= File.basename(filename)
    @objects[key].write(:file => filename)
  end

  def ls(key)
    get_object_description(@objects[key]) if @objects[key].exists?
  end

  def ls_prefix(key_prefix)
    @objects.with_prefix(key_prefix).map {|obj| get_object_description(obj) }
  end

  def ls_substring(key_substring, key_prefix=nil)
    descriptions = []
    objects = key_prefix ? @objects.with_prefix(key_prefix) : @objects

    objects.each do |obj|
      descriptions << get_object_description(obj) if obj.key =~ /#{key_substring}/
    end

    descriptions
  end

  def ls_regexp(key_regexp)
    descriptions = []

    @objects.each do |obj|
      descriptions << get_object_description(obj) if obj.key =~ key_regexp
    end

    descriptions
  end

  def mv(src_key, dst_key, dst_bucket=@bucket.name)
    @objects[src_key].move_to(dst_key, :bucket_name => dst_bucket)
  end

  def rm(key, options={})
    @objects[key].delete
  end

  def rm_prefix(key_prefix)
    @objects.with_prefix(key_prefix).each {|obj| obj.delete }
  end

  def rm_substring(key_substring, key_prefix=nil)
    objects = key_prefix ? @objects.with_prefix(key_prefix) : @objects

    objects.each do |obj|
      obj.delete if obj.key =~ /#{key_substring}/
    end
  end

  def rm_regexp(key_regexp)
    @objects.each do |obj|
      obj.delete if obj.key =~ key_regexp
    end
  end

  def rm_if(&block)
    @objects.delete_if(&block)
  end

  private

  def get_object_description(obj)
    "#{obj.key}\t#{obj.last_modified.utc}\t#{obj.content_length / 1024 / 1024} MB"
  end
end
