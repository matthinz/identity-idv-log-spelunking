# frozen_string_literal: true

require 'csv'
require 'ruby-progressbar'
require 'sqlite3'
require_relative '../lib/journey_row'
require_relative '../lib/journey_tracer'

AGREEMENT_SUBMITTED_EVENT = 'IdV: doc auth agreement submitted'
AGREEMENT_VISITED_EVENT = 'IdV: doc auth agreement visited'
FINAL_RESOLUTION_EVENT = 'IdV: final resolution'
GETTING_STARTED_SUBMITTED_EVENT = 'IdV: doc auth getting_started submitted'
GETTING_STARTED_VISITED_EVENT = 'IdV: doc auth getting_started visited'
WELCOME_SUBMITTED_EVENT = 'IdV: doc auth welcome submitted'
WELCOME_VISITED_EVENT = 'IdV: doc auth welcome visited'

INTRO_VISITED_EVENT = 'IdV: intro visited'

EVENTS_TABLE = 'logs'
JOURNEYS_TABLE = 'journeys'

# Looks at a user journey
class UserJourneyLookerAtter
  def db
    @db ||= begin
      db = SQLite3::Database.new 'logs.db'
      db.results_as_hash = true

      db.execute <<~SQL
        DROP TABLE IF EXISTS #{JOURNEYS_TABLE};
      SQL

      db
    end
  end

  def each_row
    sql = <<~SQL
      SELECT *
      FROM #{EVENTS_TABLE}
      WHERE
        user_id != 'anonymous-uuid'
      ORDER BY
        user_id,
        timestamp
    SQL

    db.execute(sql) do |event|
      prepare_event(event)
      yield event
    end
  end

  def create_journeys_table(row)
    return if @created_table

    @created_table = true

    db.execute(
      <<~SQL
        CREATE TABLE #{JOURNEYS_TABLE} (
          #{row.keys.join(', ')}
        )
      SQL
    )

    db.execute(
      <<~SQL
        CREATE INDEX ix_journey_user_id_timestamp ON #{JOURNEYS_TABLE} (user_id, timestamp);
      SQL
    )
  end

  def event_count
    db.execute(
      <<~SQL
        SELECT COUNT(DISTINCT user_id) AS count FROM #{EVENTS_TABLE} WHERE user_id != \'anonymous-uuid\'
      SQL
    )[0]['count'].to_i
  end

  def flush
    progress_bar.increment

    process_current_user_events

    @current_user_id = nil
    @current_user_events = []
  end

  def insert_journey_row(row)
    create_journeys_table(row)

    sql = <<~SQL
      INSERT INTO
        #{JOURNEYS_TABLE} (#{row.keys.join(',')})
      VALUES (#{row.keys.map { '?' }.join(',')})
    SQL

    db.transaction unless db.transaction_active?
    db.execute(sql, row.values.map { |value| value_to_sqlite(value) })
  end

  def journey_count
    db.execute(
      <<~SQL
        SELECT COUNT(*) AS count FROM #{JOURNEYS_TABLE};
      SQL
    )[0]['count'].to_i
  end

  def prepare_event(event)
    boolean_fields = %w[
      browser_bot
      browser_mobile
      fraud_rejection
      fraud_review_pending
      gpo_verification_pending
      in_person_verification_pending
      new_event
      success
    ]

    boolean_fields.each do |field|
      next if event[field].nil?

      event[field] = event[field].to_i != 0
    end
    event['timestamp'] = DateTime.parse(event['timestamp']).to_time
    event
  end

  def process_current_user_events
    return if @current_user_events.empty?

    tracer = JourneyTracer.new(
      start_event_names: [WELCOME_VISITED_EVENT, GETTING_STARTED_VISITED_EVENT],
      end_event_name: FINAL_RESOLUTION_EVENT
    )

    tracer.trace(@current_user_events).each do |journey|
      row = JourneyRow.new(journey)
      insert_journey_row(row.to_h)
    end
  end

  def progress_bar
    @progress_bar ||= ProgressBar.create(
      format: '%P%% (%e) |%B|',
      output: $stderr,
      total: event_count
    )
  end

  def run
    @current_user_id = nil
    @current_user_events = []

    each_row do |row|
      flush if row['user_id'] != @current_user_id

      @current_user_id = row['user_id']
      @current_user_events << row
    end

    flush

    db.commit if db.transaction_active?

    track_subsequent_journeys(db)
    puts "Found #{journey_count} journeys"
  end

  def track_subsequent_journeys(db)
    db.execute <<~SQL
      ALTER TABLE #{JOURNEYS_TABLE} ADD COLUMN subsequent_journeys INT NULL;
    SQL

    db.execute <<~SQL
      ALTER TABLE #{JOURNEYS_TABLE} ADD COLUMN eventual_idv_success INT NULL;
    SQL

    db.execute <<~SQL
      UPDATE #{JOURNEYS_TABLE} AS parent
      SET
        subsequent_journeys = (SELECT COUNT(*) FROM #{JOURNEYS_TABLE} AS child WHERE child.user_id = parent.user_id AND child.timestamp > parent.timestamp),
        eventual_idv_success = (SELECT MIN(COUNT(*),1) FROM #{JOURNEYS_TABLE} AS child WHERE child.user_id = parent.user_id AND child.timestamp > parent.timestamp AND child.idv_success = 1)
      ;
    SQL
  end

  def value_to_sqlite(value)
    case value
    when Time, Numeric
      value.to_i
    when TrueClass, FalseClass
      value ? 1 : 0
    else
      value.to_s
    end
  end
end

UserJourneyLookerAtter.new.run
