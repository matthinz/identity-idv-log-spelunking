require 'csv'
require 'ruby-progressbar'
require 'sqlite3'

AGREEMENT_SUBMITTED_EVENT = 'IdV: doc auth agreement submitted'
AGREEMENT_VISITED_EVENT = 'IdV: doc auth agreement visited'
FINAL_RESOLUTION_EVENT = 'IdV: final resolution'
GETTING_STARTED_SUBMITTED_EVENT = 'IdV: doc auth getting_started submitted'
GETTING_STARTED_VISITED_EVENT = 'IdV: doc auth getting_started visited'
WELCOME_SUBMITTED_EVENT = 'IdV: doc auth welcome submitted'
WELCOME_VISITED_EVENT = 'IdV: doc auth welcome visited'

ALL_EVENTS = [
  AGREEMENT_SUBMITTED_EVENT,
  AGREEMENT_VISITED_EVENT,
  FINAL_RESOLUTION_EVENT,
  GETTING_STARTED_SUBMITTED_EVENT,
  GETTING_STARTED_VISITED_EVENT,
  WELCOME_SUBMITTED_EVENT,
  WELCOME_VISITED_EVENT
].freeze

START_DATE = '2023-08-01 17:06'

END_DATE = '2023-08-08 18:50'

db = SQLite3::Database.new 'logs.db'
db.results_as_hash = true

def getting_started_bucket_results(db, &block)
  sql = <<~SQL
    SELECT
      SUM(name = 'IdV: doc auth getting_started visited' and new_event = '1') as getting_started,
      SUM(name = 'IdV: doc auth getting_started submitted' and new_event = '1') as getting_started_submitted,
      SUM(name = 'IdV: doc auth document_capture visited' and new_event = '1') as document_capture,
      SUM(name = 'IdV: doc auth document_capture visited' and flow_path = 'standard' and new_event = '1') as document_capture_standard,
      SUM(name = 'IdV: doc auth document_capture visited' and flow_path = 'hybrid' and new_event = '1') as document_capture_hybrid,
      SUM(name = 'IdV: doc auth ssn visited' and new_event = '1') as verify_info,
      SUM(name = 'IdV: phone of record visited' and new_event = '1') as phone_or_address,
      SUM(name = 'IdV: review info visited' and new_event = '1') as secure_account,
      SUM(name = 'IdV: final resolution' and new_event = '1') as workflow_complete
    FROM
      logs
    WHERE
      bucket = 'getting_started'
      AND
      timestamp >= ?
      AND
      timestamp <= ?
  SQL
  db.execute(sql, [START_DATE, END_DATE], &block)
end

def welcome_bucket_results(db, &block)
  sql = <<~SQL
    SELECT
      SUM(name = 'IdV: doc auth welcome visited' and new_event = '1') as welcome,
      SUM (name = 'IdV: doc auth agreement submitted' and new_event = '1') as agreement_submitted,
      SUM (name = 'IdV: doc auth document_capture visited' and new_event = '1') as document_capture,
      SUM (name = 'IdV: doc auth document_capture visited' and flow_path = 'standard' and new_event = '1') as document_capture_standard,
      SUM (name = 'IdV: doc auth document_capture visited' and flow_path = 'hybrid' and new_event = '1') as document_capture_hybrid,
      SUM (name = 'IdV: doc auth ssn visited' and new_event = '1') as verify_info,
      SUM (name = 'IdV: phone of record visited' and new_event = '1') as phone_or_address,
      SUM (name = 'IdV: review info visited' and new_event = '1') as secure_account,
      SUM (name = 'IdV: final resolution' and new_event = '1') as workflow_complete
    FROM
      logs
    WHERE
      bucket = 'welcome'
      AND
      timestamp >= ?
      AND
      timestamp <= ?
  SQL
  db.execute(sql, [START_DATE, END_DATE], &block)
end

csv = CSV.new(STDOUT)

welcome_bucket_results(db) do |results|
  csv << results.keys
  csv << results.values
end

getting_started_bucket_results(db) do |results|
  csv << results.keys
  csv << results.values
end
