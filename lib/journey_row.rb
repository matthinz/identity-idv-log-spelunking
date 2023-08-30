class JourneyRow
  AGREEMENT_SUBMITTED_EVENT = 'IdV: doc auth agreement submitted'
  AGREEMENT_VISITED_EVENT = 'IdV: doc auth agreement visited'
  FINAL_RESOLUTION_EVENT = 'IdV: final resolution'
  GETTING_STARTED_SUBMITTED_EVENT = 'IdV: doc auth getting_started submitted'
  GETTING_STARTED_VISITED_EVENT = 'IdV: doc auth getting_started visited'
  WELCOME_SUBMITTED_EVENT = 'IdV: doc auth welcome submitted'
  WELCOME_VISITED_EVENT = 'IdV: doc auth welcome visited'

  COLUMNS = %w[
    user_id
    timestamp
    bucket
    idv_success
    idv_success_but_gpo_pending
    idv_success_but_in_person_pending
    attempted_hybrid_handoff
    bounced
    caught_by_threatmetrix
    clicked_help_link
    changed_browser
    changed_device
    desktop_browser
    desktop_device
    desktop_only
    did_hybrid_handoff
    document_capture_attempts
    document_capture_success
    document_type
    last_event
    length
    locale
    mobile_browser
    mobile_device
    mobile_only
    path
    saw_hybrid_handoff
    service_provider

  ].freeze

  def initialize(events)
    @events = events
  end

  def bucket
    case @events.first['name']
    when WELCOME_VISITED_EVENT
      'welcome'
    when GETTING_STARTED_VISITED_EVENT
      'getting_started'
    else
      raise 'Invalid first event in journey'
    end
  end

  def attempted_hybrid_handoff
    !!@events.find do |event|
      event['name'] == 'IdV: doc auth hybrid handoff submitted' && event['flow_path'] == 'hybrid'
    end
  end

  def bounced
    submit_event = case bucket
                   when 'welcome'
                     'IdV: doc auth agreement submitted'
                   when 'getting_started'
                     'IdV: doc auth getting_started submitted'
                   end

    @events.none? { |event| event['name'] == submit_event } && !idv_success
  end

  def caught_by_threatmetrix
    !!@events.find do |event|
      event['threatmetrix_review_status'] == 'reject' || event['threatmetrix_review_status'] == 'review'
    end
  end

  def changed_browser
    devices = {}
    @events.each do |event|
      devices[event['browser_name']] = true
    end
    devices.keys.length > 1
  end

  def changed_device
    devices = {}
    @events.each do |event|
      devices[event['browser_device_name']] = true
    end
    devices.keys.length > 1
  end

  def clicked_help_link
    !!@events.find do |event|
      event['name'] == 'External Redirect' and event['redirect_url'].to_s.include?('https://www.login.gov/help')
    end
  end

  def desktop_only
    @events.all? { |event| !event['browser_mobile'] }
  end

  def desktop_browser
    most_popular_attr('browser_name', allow_nil: true) { |event| !event['browser_mobile'] }
  end

  def desktop_device
    most_popular_attr('browser_device', allow_nil: true) { |event| !event['browser_mobile'] }
  end

  def did_hybrid_handoff
    @events.any? do |event|
      next unless event['name'] == 'IdV: doc auth hybrid handoff submitted' && event['flow_path'] == 'hybrid'

      # we have a hybrid handoff event, now look for a subsequent doc capture visit
      @events.any? do |e|
        e['timestamp'] > event['timestamp'] && e['name'] == 'IdV: doc auth document_capture visited' && e['flow_path'] == 'hybrid'
      end
    end
  end

  def document_capture_attempts
    @events.count do |event|
      event['name'] == 'IdV: doc auth document_capture submitted'
    end
  end

  def document_capture_success
    @events.any? do |event|
      event['name'] == 'IdV: doc auth verify proofing results' && event['success']
    end
  end

  def document_type
    result = nil

    # Let the last document type they tried win
    @events.each do |event|
      result = event['doc_class'] if event['name'] == 'IdV: doc auth image upload vendor submitted'
    end

    result
  end

  def last_event
    @events.last['name']
  end

  def length
    @events.length
  end

  def locale
    most_popular_attr('locale')
  end

  def mobile_browser
    most_popular_attr('browser_name', allow_nil: true) { |event| event['browser_mobile'] }
  end

  def mobile_device
    most_popular_attr('browser_device', allow_nil: true) { |event| event['browser_mobile'] }
  end

  def mobile_only
    @events.all? { |event| event['browser_mobile'].nil? || event['browser_mobile'] }
  end

  def path
    @events.map { |event| clean_event_name(event, shorten: true) }.join(' -> ')
  end

  def saw_hybrid_handoff
    !!@events.any? do |event|
      event['name'] == 'IdV: doc auth hybrid handoff visited'
    end
  end

  def service_provider
    most_popular_attr 'service_provider', ignore_blank: true, allow_nil: true
  end

  def idv_success
    @events.any? do |event|
      event['name'] == FINAL_RESOLUTION_EVENT && event['success'] && !event['gpo_verification_pending'] && !event['in_person_verification_pending']
    end
  end

  def idv_success_but_gpo_pending
    @events.any? do |event|
      event['name'] == FINAL_RESOLUTION_EVENT && event['success'] && event['gpo_verification_pending']
    end
  end

  def idv_success_but_in_person_pending
    @events.any? do |event|
      event['name'] == FINAL_RESOLUTION_EVENT && event['success'] && event['deactivation_reason'] == 'in_person_verification_pending'
    end
  end

  def timestamp
    @events.first['timestamp']
  end

  def to_h
    result = {}
    COLUMNS.each do |column|
      result[column.to_s] = send(column)
    end
    result
  end

  def user_id
    common_attr 'user_id'
  end

  private

  def clean_event_name(event, shorten: true)
    name = event['name']

    if shorten
      name = name
             .sub('IdV: doc auth ', '')
             .sub('visited', 'visit')
             .sub('submitted', 'submit')
    end

    attrs = []

    # Return to SP has really long redirect_url values we don't care about
    attrs << event['redirect_url'] if event['redirect_url'] && !name.include?('Return to SP')

    attrs << case event['success']
             when true
               'success'
             when false
               'failure'
             end

    attrs.compact!

    name = "#{name} (#{attrs.join(', ')})" unless attrs.empty?
    name
  end

  def common_attr(field, ignore_blank: false, allow_nil: false, &block)
    attrs = index_attrs(field, ignore_blank:, &block)

    case attrs.length
    when 0
      raise "No values for #{field}" unless allow_nil

      nil
    when 1
      attrs.keys.first
    else
      @events.each do |event|
        warn("#{event['user_id']} #{event['timestamp']}: #{event['name']} (#{event[field].inspect})")
      end

      values = attrs.keys.map(&:inspect).join(',')
      raise "Too many values for #{field}: #{values}"
    end
  end

  def index_attrs(field, ignore_blank: false)
    attrs = {}
    @events.each do |event|
      value = event[field].to_s
      next if value.empty? && ignore_blank
      next if block_given? && !(yield event)

      attrs[value] ||= 0
      attrs[value] += 1
    end
    attrs
  end

  def most_popular_attr(field, ignore_blank: false, allow_nil: false)
    attrs = index_attrs(field, ignore_blank:)

    result = nil
    highest_count = nil
    attrs.each_pair do |attr, count|
      if highest_count.nil? || count > highest_count
        result = attr
        highest_count = count
      end
    end

    raise "No value found for #{field}" if result.nil? && !allow_nil

    result
  end
end
