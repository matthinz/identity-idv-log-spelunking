require 'json'
require 'sqlite3'

DB = 'logs.db'.freeze
TABLE = 'logs'.freeze

def init_db(columns)
  db = SQLite3::Database.new DB

  # db.execute <<-SQL
  # DROP TABLE IF EXISTS #{TABLE};
  # SQL

  db.execute <<-SQL
  CREATE TABLE IF NOT EXISTS #{TABLE} (#{columns.join(',')})
  SQL

  db
end

def add_column(db, column)
  transaction_was_active = db.transaction_active?

  db.commit if transaction_was_active

  begin
    db.execute <<-SQL
    ALTER TABLE #{TABLE} ADD COLUMN #{column};
    SQL
  rescue SQLite3::SQLException
    # Assume it already exists
  end

  return unless transaction_was_active

  db.transaction
end

db = nil
columns = ['timestamp']
insert_statement = nil

$stdin.each_line do |line|
  row = JSON.parse(line)
  row['timestamp'] = row.delete('@timestamp')

  row.each_key do |column|
    next if columns.include?(column)

    warn "Adding column: #{column}"
    columns << column

    if db
      add_column(db, column)
      insert_statement = nil
    else
      db = init_db(columns)
    end
  end

  insert_statement ||= db.prepare <<-SQL
    INSERT INTO logs (
      #{columns.join(',')}
    ) VALUES (
      #{columns.map { '?' }.join(',')}
    )
  SQL

  # Running lots of inserts inside a transaction is way, way faster than not (because SQLite will wrap each individual in its own transaction otherwise)
  db.transaction unless db.transaction_active?

  values = columns.map { |column| row[column] }
  insert_statement.execute(values)
end

db.commit if db.transaction_active?
