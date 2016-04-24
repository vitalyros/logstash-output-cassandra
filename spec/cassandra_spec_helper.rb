# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/event'
require 'simplecov'
require 'simplecov-rcov'

SimpleCov.formatter = SimpleCov::Formatter::MultiFormatter.new([
  SimpleCov::Formatter::HTMLFormatter,
  SimpleCov::Formatter::RcovFormatter
])

SimpleCov.start do
  add_filter '/spec/'
end
