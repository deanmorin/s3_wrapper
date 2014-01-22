# encoding=utf-8
require 'aws-sdk'
require_relative '../lib/s3_wrapper.rb'


KEY = 's3_wrapper_test_file'
KEY2 = 's3_wrapper_test_file2'
KEY3 = 's3_wrapper_test_file3'
NON_MATCHING_PREFIX = 'non_matching_s3_wrapper_test'
SUBSTRING = 'wrapper_test'
PREFIX = 's3'
REGEXP = /^s3.*wrapper_test.*/
FILENAME = '/tmp/s3_wrapper_test_file'
TEXT = 'sample_text'


describe S3Wrapper do

  before(:all) do
    s3 = AWS::S3.new(
      :aws_access_key_id => ENV['AWS_ACCESS_KEY_ID'],
      :aws_secret_access_key => ENV['AWS_SECRET_ACCESS_KEY']
    )
    @bucket = s3.buckets[ENV['S3_WRAPPER_TEST_BUCKET']]
    @bucket_alt = s3.buckets[ENV['S3_WRAPPER_TEST_BUCKET_ALT']]
    @s3_wrapper = S3Wrapper.new(ENV['S3_WRAPPER_TEST_BUCKET'])
  end

  after(:each) do
    @bucket.objects.delete(KEY)
    @bucket.objects.delete(KEY2)
    @bucket.objects.delete(KEY3)
    @bucket.objects.delete(NON_MATCHING_PREFIX)
    File.delete(FILENAME) if File.exist?(FILENAME)
  end

  describe '#put' do

    it 'puts data into a file in the bucket' do

      @s3_wrapper.put(KEY, TEXT)

      expect(@bucket.objects[KEY].exists?).to be_true
      expect(@bucket.objects[KEY].read).to eq(TEXT)
    end
  end

  describe '#put_file' do

    it 'adds a file to the bucket' do

      File.open(FILENAME, 'wb') do |fh|
        fh.write(TEXT)
      end
      @s3_wrapper.put_file(FILENAME)

      expect(@bucket.objects[KEY].read).to eq(TEXT)
    end
  end

  describe '#get' do

    it 'retrieves a file from the bucket as a string' do

      @bucket.objects.create(KEY, TEXT)
      result = @s3_wrapper.get(KEY)

      expect(result).to be_a(String)
      expect(result).to eq(TEXT)
    end
  end

  describe '#get_file' do

    it 'retrieves a file from the bucket' do

      @bucket.objects.create(KEY, TEXT)
      @s3_wrapper.get_file(KEY, FILENAME)
      result = nil

      File.open(FILENAME) do |fh|
        result = fh.readline
      end
      expect(result).to eq(TEXT)
    end
  end

  describe '#ls' do

    it 'gives a description of a file' do

      @bucket.objects.create(KEY, TEXT)
      result = @s3_wrapper.ls(KEY)

      expect(result).to match(/#{KEY}\t[0-9-]+ [0-9:]+ UTC\t0 MB/)
    end

    it 'returns nil if there are no matches' do

      result = @s3_wrapper.ls('DOES_NOT_EXIST')

      expect(result).to be_nil
    end
  end

  describe '#ls_prefix' do

    it 'gives a description of each file with the given prefix' do

      @bucket.objects.create(KEY, TEXT)
      @bucket.objects.create(KEY2, TEXT)
      @bucket.objects.create(KEY3, TEXT)
      @bucket.objects.create(NON_MATCHING_PREFIX, TEXT)
      result = @s3_wrapper.ls_prefix(KEY)

      expect(result.length).to eq(3)
      result.each do |r|
        expect(valid_ls_description?(r)).to be_true
      end
    end

    it 'returns an empty array if there are no matches' do

      result = @s3_wrapper.ls_prefix('DOES_NOT_EXIST')

      expect(result).to match_array([])
    end
  end

  describe '#ls_substring' do

    it 'gives a description of each matching file' do

      @bucket.objects.create(KEY, TEXT)
      @bucket.objects.create(KEY2, TEXT)
      @bucket.objects.create(KEY3, TEXT)
      @bucket.objects.create(NON_MATCHING_PREFIX, TEXT)
      result = @s3_wrapper.ls_substring(SUBSTRING)

      expect(result.length).to eq(4)
      result.each do |r|
        expect(valid_ls_description?(r)).to be_true
      end
    end

    it 'gives a description of each matching file with the given prefix' do

      @bucket.objects.create(KEY, TEXT)
      @bucket.objects.create(KEY2, TEXT)
      @bucket.objects.create(KEY3, TEXT)
      @bucket.objects.create(NON_MATCHING_PREFIX, TEXT)
      result = @s3_wrapper.ls_substring(SUBSTRING, PREFIX)

      expect(result.length).to eq(3)
      result.each do |r|
        expect(valid_ls_description?(r)).to be_true
      end
    end

    it 'returns an empty array if there are no matches' do

      result = @s3_wrapper.ls_substring('DOES_NOT_EXIST')

      expect(result).to match_array([])
    end
  end

  describe '#ls_regexp' do

    it 'gives a description of each file matching the regular expression' do
      @bucket.objects.create(KEY, TEXT)
      @bucket.objects.create(KEY2, TEXT)
      @bucket.objects.create(KEY3, TEXT)
      @bucket.objects.create(NON_MATCHING_PREFIX, TEXT)
      result = @s3_wrapper.ls_substring(REGEXP)

      expect(result.length).to eq(3)
      result.each do |r|
        expect(valid_ls_description?(r)).to be_true
      end
    end

    it 'returns an empty array if there are no matches' do

      result = @s3_wrapper.ls_regexp(/DOES_NOT_EXIST/)

      expect(result).to match_array([])
    end
  end

  describe '#mv' do

    it 'changes the key of a file' do
      @bucket.objects.create(KEY, TEXT)
      @s3_wrapper.mv(KEY, KEY2)

      expect(@bucket.objects[KEY2].read).to eq(TEXT)
    end

    it 'moves a file to a different bucket' do
      @bucket.objects.create(KEY, TEXT)
      @s3_wrapper.mv(KEY, KEY, ENV['S3_WRAPPER_TEST_BUCKET_ALT'])

      expect(@bucket_alt.objects[KEY].read).to eq(TEXT)

      @bucket_alt.objects.delete(KEY)
    end
  end

  describe '#rm' do

    it 'deletes a file' do

      @bucket.objects.create(KEY, TEXT)
      @s3_wrapper.rm(KEY)

      expect(@bucket.objects[KEY].exists?).to be_false
    end
  end

  describe '#rm_prefix' do

    it 'deletes all files with the given prefix' do

      @bucket.objects.create(KEY, TEXT)
      @bucket.objects.create(KEY2, TEXT)
      @bucket.objects.create(KEY3, TEXT)
      @bucket.objects.create(NON_MATCHING_PREFIX, TEXT)
      @s3_wrapper.rm_prefix(PREFIX)

      expect(@bucket.objects[KEY].exists?).to be_false
      expect(@bucket.objects[KEY2].exists?).to be_false
      expect(@bucket.objects[KEY3].exists?).to be_false
      expect(@bucket.objects[NON_MATCHING_PREFIX].exists?).to be_true
    end
  end

  describe '#rm_substring' do

    it 'deletes all matching files' do

      @bucket.objects.create(KEY, TEXT)
      @bucket.objects.create(KEY2, TEXT)
      @bucket.objects.create(KEY3, TEXT)
      @bucket.objects.create(NON_MATCHING_PREFIX, TEXT)
      @s3_wrapper.rm_substring(SUBSTRING)

      expect(@bucket.objects[KEY].exists?).to be_false
      expect(@bucket.objects[KEY2].exists?).to be_false
      expect(@bucket.objects[KEY3].exists?).to be_false
      expect(@bucket.objects[NON_MATCHING_PREFIX].exists?).to be_false
    end

    it 'deletes all matching files with the given prefix' do

      @bucket.objects.create(KEY, TEXT)
      @bucket.objects.create(KEY2, TEXT)
      @bucket.objects.create(KEY3, TEXT)
      @bucket.objects.create(NON_MATCHING_PREFIX, TEXT)
      @s3_wrapper.rm_substring(SUBSTRING, PREFIX)

      expect(@bucket.objects[KEY].exists?).to be_false
      expect(@bucket.objects[KEY2].exists?).to be_false
      expect(@bucket.objects[KEY3].exists?).to be_false
      expect(@bucket.objects[NON_MATCHING_PREFIX].exists?).to be_true
    end
  end

  describe '#rm_regexp' do

    it 'deletes all files matching the regular expression' do
      @bucket.objects.create(KEY, TEXT)
      @bucket.objects.create(KEY2, TEXT)
      @bucket.objects.create(KEY3, TEXT)
      @bucket.objects.create(NON_MATCHING_PREFIX, TEXT)
      @s3_wrapper.rm_substring(REGEXP, PREFIX)

      expect(@bucket.objects[KEY].exists?).to be_false
      expect(@bucket.objects[KEY2].exists?).to be_false
      expect(@bucket.objects[KEY3].exists?).to be_false
      expect(@bucket.objects[NON_MATCHING_PREFIX].exists?).to be_true
    end
  end

  describe '#rm_if' do
    
    it 'deletes files according to the block passed in' do
      @bucket.objects.create(KEY, TEXT)
      @bucket.objects.create(KEY2, TEXT)
      @s3_wrapper.rm_if {|obj| obj.key == KEY }

      expect(@bucket.objects[KEY].exists?).to be_false
      expect(@bucket.objects[KEY2].exists?).to be_true
    end
  end
end

def valid_ls_description?(description)
  description =~ /.*\t[0-9-]+ [0-9:]+ UTC\t0 MB/
end
