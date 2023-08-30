# "Welcome" vs. "Getting Started"

The numbers below are in terms of *journeys* over the dates **8/1/23 - 8/8/23 (UTC)**. A journey is a set of events for a user that starts with the relevant visit event (“welcome visited” or “getting_started visited”) and continues until one of the following occurs:

* The user completes IdV (we see an “IdV: final resolution” event)
* The user starts a new journey (we see another “welcome” or “getting_started” visited event)
* There is large gap of time between events (I chose 1 hour)

Journeys are bucketed using the initiating event:

| Bucket | Initiating Event |
| -- | -- |
| `welcome` | IdV: doc auth welcome visited |
| `getting_started` | IdV: doc auth getting_started visited |

## What makes a good journey?

If you make it through document capture, odds are good you'll make it through IdV.

%%document_capture_success%%

### Notes

- Completing IdV without making it past doc capture here indicates a failure in event tracking.

## Overall A/B test results

Looking at the data in terms of journeys, it looks like `welcome` remains the winner:

%%overall%%

## By language

%%by_locale%%

## By service provider

%%by_sp%%

### Notes

- "(None)" indicates that the user completed the IdV attempt in a separate session than their initial request. In these cases, we redirect the user back to their account page after IdV.

## By document type

%%by_document_type%%


### Notes

- Document type is the _last_ document type used during the journey.


## Mobile & hybrid handoff

### Users who attempted hybrid handoff

%%attempted_hybrid_handoff%%

### Users who only ever used a mobile device

%%mobile_only%%

### Users who only ever used a desktop device

%%desktop_only%%

## Bounce rates

We consider a journey a "bounce" if:

* It does not include a "submitted" event indicating the user progressed past the screen containing the IdV consent checkbox.
* The user also does not progress past the IdV consent checkbox in a _subsequent_ journey

%%bounces%%
