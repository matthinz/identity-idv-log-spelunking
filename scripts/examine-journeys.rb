require 'sqlite3'
require 'terminal-table'

BASELINE_BUCKET = 'welcome'
MIN_COUNT = 70
JOURNEYS_TABLE = 'journeys'

def pct(value)
  "#{((value || 0) * 100).round(2)}%"
end

def build_bucket_sql(group_by: nil, labels: nil, include_ab_test: nil, **_extra)
  group_by = [
    *group_by,
    include_ab_test == false ? nil : 'bucket'
  ].compact

  labels ||= {}

  group_by
    .map do |column|
      column_as_str = column.to_s
      column_as_sym = column_as_str.to_sym

      labels_for_column = labels[column_as_str] || labels[column_as_sym]

      unless labels_for_column
        next [
          "CASE #{column}",
          "WHEN 1 THEN 'yes'",
          "WHEN 0 THEN 'no'",
          "ELSE #{column}",
          'END'
        ].join(' ')
      end

      [
        "CASE #{column}",
        *labels_for_column.keys.map do |key|
          literal_key = key.is_a?(Numeric) ? key : "'#{key}'"
          "WHEN #{literal_key} THEN '#{labels_for_column[key]}'"
        end,
        "ELSE #{column}",
        'END'
      ].join(' ')
    end
    .join(" || ' / ' || ")
end

def get_journey_metrics(
  db,
  options
)

  criteria = [
    *(options[:where].is_a?(Array) ? options[:where] : [options[:where]]),
    '1'
  ].compact.join(' AND ')

  bucket_sql = build_bucket_sql(**options)

  min_count = options[:min_count] || MIN_COUNT

  db.execute(
    <<~SQL
      SELECT
        #{bucket_sql} AS bucket,
        COUNT(*) AS count,
        (SELECT COUNT(*) FROM #{JOURNEYS_TABLE} AS child WHERE bucket = parent.bucket) AS bucket_count,
        CAST(COUNT(*) AS float) / (SELECT COUNT(*) FROM #{JOURNEYS_TABLE} AS child WHERE bucket = parent.bucket) AS bucket_rate,
        SUM(idv_success) AS idv_success_count,
        CAST(SUM(idv_success) as float) / COUNT(*) AS idv_success_rate,
        SUM(idv_success_but_gpo_pending) AS idv_success_but_gpo_pending_count,
        CAST(SUM(idv_success_but_gpo_pending) as float) / COUNT(*) AS idv_success_but_gpo_pending_rate,
        SUM(idv_success_but_in_person_pending) AS idv_success_but_in_person_pending_count,
        CAST(SUM(idv_success_but_in_person_pending) as float) / COUNT(*) AS idv_success_but_in_person_pending_rate,
        SUM(IIF(document_capture_attempts > 0, 1, 0)) AS document_capture_attempt_count,
        CAST(SUM(IIF(document_capture_attempts > 0, 1, 0)) AS float) / COUNT(*) AS document_capture_attempt_rate,
        SUM(document_capture_success) AS document_capture_success_count,
        CAST(SUM(document_capture_success) AS float) / SUM(IIF(document_capture_attempts > 0, 1, 0)) AS document_capture_success_rate
      FROM
        #{JOURNEYS_TABLE} AS parent
      WHERE #{criteria}
      GROUP BY
        #{bucket_sql}
      HAVING COUNT(*) > #{min_count}
      ORDER BY #{bucket_sql}
    SQL
  )
end

def build_table(db, table)
  metrics = get_journey_metrics(db, table)

  success_rate_columns = %w[
    document_capture_success_rate
    idv_success_rate
    idv_success_but_gpo_pending_rate
    idv_success_but_in_person_pending_rate
  ]

  Terminal::Table.new do |t|
    t.style = { border: :markdown }

    t << build_table_headers(table)
    t << :separator

    best_success_rates_by_bucket = {}

    metrics.each do |row|
      # Buckets are i.e. "thing / welcome" and "thing / getting_started"
      # The idea is we always want to comparing welcome vs getting started
      bucket_for_comparison = row['bucket'].split('/')[0...-1].join('/')
      best_success_rates_by_bucket[bucket_for_comparison] ||= {}

      success_rate_columns.each do |column|
        best = best_success_rates_by_bucket[bucket_for_comparison][column]
        best_success_rates_by_bucket[bucket_for_comparison][column] = row[column] if best.nil? || row[column] > best
      end

      row['bucket_for_comparison'] = bucket_for_comparison
    end

    metrics.each do |row|
      t << build_table_row(table, row, best_success_rates_by_bucket:)
    end
  end
end

def build_table_headers(table)
  headers = ['', 'Journeys']

  headers = [*headers, 'IdV successes', 'IdV success rate'] if table[:include_idv_success] != false

  if table[:include_doc_capture]
    headers = [
      *headers,
      'Doc capture attempts', 'Doc capture attempt rate', 'Doc capture successes', 'Doc capture success rate'
    ]
  end

  headers = [*headers, 'Gpo pending', 'Gpo pending rate'] if table[:include_gpo] != false
  headers = [*headers, 'Ipp pending', 'Ipp pending rate'] if table[:include_ipp] != false

  headers << 'Rate' if table[:include_bucket_rate]

  headers
end

def format_success_rate(row, column:, best_success_rates_by_bucket:)
  return '' if row[column].nil?

  result = pct(row[column])

  delta = row[column] - best_success_rates_by_bucket[row['bucket_for_comparison']][column]

  return result unless delta.negative?

  [
    result,
    "<span style=\"color: #f00\">#{pct(delta)}</span>"
  ].join(' ')
end

def build_table_row(table, row, best_success_rates_by_bucket:)
  table_row = [
    row['bucket'],
    row['count']
  ]

  if table[:include_idv_success] != false
    table_row = [
      *table_row,
      row['idv_success_count'],
      if table[:show_success_rate_diff] == false
        pct(row['idv_success_rate'])
      else
        format_success_rate(row, column: 'idv_success_rate', best_success_rates_by_bucket:)
      end
    ]
  end

  if table[:include_doc_capture]
    table_row = [
      *table_row,
      row['document_capture_attempt_count'],
      pct(row['document_capture_attempt_rate']),
      row['document_capture_success_count'],
      pct(row['document_capture_success_rate'])
    ]
  end

  if table[:include_gpo] != false
    table_row = [
      *table_row,
      row['idv_success_but_gpo_pending_count'],
      format_success_rate(row, column: 'idv_success_but_gpo_pending_rate', best_success_rates_by_bucket:)
    ]
  end

  if table[:include_ipp] != false
    table_row = [
      *table_row,
      row['idv_success_but_in_person_pending_count'],
      format_success_rate(row, column: 'idv_success_but_in_person_pending_rate', best_success_rates_by_bucket:)
    ]
  end

  table_row << pct(row['bucket_rate']) if table[:include_bucket_rate]

  table_row
end

tables = {
  overall: {
    title: 'Overall'
  },
  document_capture_success: {
    title: 'Document capture success',
    group_by: 'document_capture_success',
    include_ab_test: false,
    include_gpo: false,
    include_ipp: false,
    show_winner: false,
    show_success_rate_diff: false,
    labels: {
      document_capture_success: {
        0 => 'did not make it to and pass doc capture',
        1 => 'passed doc capture'
      }
    }
  },
  by_locale: {
    title: 'By locale',
    description: <<~TEXT,
      The locale used for the most events in a journey wins. That is, if a user
      starts in English but transitions to Spanish for most of the journey,
      the locale for the journey will be Spanish.
    TEXT
    group_by: 'locale',

    labels: {
      locale: {
        'en' => 'English',
        'fr' => 'French',
        'es' => 'Spanish'
      }
    }
  },
  by_sp: {
    title: 'By service provider',
    group_by: 'service_provider',
    labels: {
      service_provider: {
        '': '(None)',
        'https://eauth.va.gov/isam/sps/saml20sp/saml20' => 'VA',
        'urn:gov:gsa:openidconnect.profiles:sp:sso:va:vassoerp' => 'VA',
        'urn:gov:gsa:SAML:2.0.profiles.profiles:sp:sso:pbgc:mypba' => 'MyPBA',
        'urn:gov:gsa:SAML:2.0.profiles:sp:sso:SSA:mySSAsp' => 'MySSA',
        'urn:gov:gsa:openidconnect.profiles:sp:sso:sba:sbaconnect' => 'SBA Connect'
      }
    },
    min_count: 1000
  },
  threatmetrix: {
    title: 'Caught by ThreatMetrix vs not',
    group_by: 'caught_by_threatmetrix',
    labels: {
      caught_by_threatmetrix: {
        0 => 'passed threatmetrix',
        1 => 'caught by threatmetrix'
      }
    }

  },
  attempted_hybrid_handoff: {
    title: 'Attempted hybrid handoff vs not',
    group_by: 'attempted_hybrid_handoff',
    labels: {
      attempted_hybrid_handoff: {
        0 => 'did not attempt hh',
        1 => 'attempted hh'
      }
    }
  },
  desktop_only: {
    title: 'Only on desktop vs not',
    group_by: 'desktop_only',
    labels: {
      desktop_only: {
        0 => 'not only desktop',
        1 => 'only desktop'
      }
    }
  },
  mobile_only: {
    title: 'Only mobile vs not',
    group_by: 'mobile_only',
    labels: {
      mobile_only: {
        0 => 'not only mobile',
        1 => 'only mobile'
      }
    }
  },
  by_document_type: {
    title: 'by document type',
    group_by: 'document_type',
    where: [
      "document_type != ''"
    ]
  },
  bounces: {
    title: 'Bounces',
    where: [
      'bounced = 1 AND subsequent_journeys = 0'
    ],
    show_winner: false,
    include_gpo: false,
    include_ipp: false,
    include_idv_success: false,
    include_bucket_rate: true
  }
}

db = SQLite3::Database.new('logs.db')
db.results_as_hash = true

if $stdin.tty?
  tables.each do |_name, table|
    puts ''
    puts table[:title]
    puts '=' * table[:title].length
    puts build_table(db, table)
  end
else
  text = $stdin.readlines.join('')
  tables.each do |name, table|
    text = text.gsub "%%#{name}%%", build_table(db, table).to_s
  end
  puts text
end
