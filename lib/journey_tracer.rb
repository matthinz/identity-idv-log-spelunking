class JourneyTracer
  attr_reader :start_event_names, :end_event_name

  def initialize(start_event_names:, end_event_name:)
    @start_event_names = start_event_names
    @end_event_name = end_event_name
  end

  def event_starts_new_journey(event)
    start_event_names.include?(event['name'])
  end

  def event_ends_journey(event:, journey_in_progress:)
    event['name'] == end_event_name
  end

  def event_is_part_of_journey(event:, journey_in_progress:)
    return false if journey_in_progress.nil?

    prev_event = journey_in_progress.last

    time_since_last_event = event['timestamp'] - prev_event['timestamp']

    return false if time_since_last_event > (1 * 60 * 60)

    true
  end

  def trace(user_events)
    journeys = []
    journey_in_progress = nil

    user_events.each do |event|
      if event_starts_new_journey(event)
        journeys << journey_in_progress if journey_in_progress
        journey_in_progress = [event]
        next
      end

      next if journey_in_progress.nil?

      if event_ends_journey(event:, journey_in_progress:)
        journey_in_progress << event
        journeys << journey_in_progress
        journey_in_progress = nil
        next
      end

      journey_in_progress << event if event_is_part_of_journey(event:, journey_in_progress:)
    end

    journeys << journey_in_progress if journey_in_progress

    journeys
  end
end
