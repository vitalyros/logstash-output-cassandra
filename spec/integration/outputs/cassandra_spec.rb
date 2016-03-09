# encoding: utf-8
require_relative "./integration_helper"
require "logstash/outputs/cassandra_output"

# TODO: add integration tests here (docker, longhorseman, et al)
describe "client create actions", :integration => true do
  let(:session) { get_session() }

  it "does nothing" do
    session = get_session()
    "hmmm...."
  end
end
