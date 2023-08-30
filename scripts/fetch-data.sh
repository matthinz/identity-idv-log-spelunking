#!/usr/bin/env bash

set -euo pipefail

IDP_ROOT=../../18f/identity-idp

function get_day {
  DAY=$1; shift

  FILENAME="results_${DAY}.ndjson.gz"

  if [ -f "$FILENAME" ]; then
    echo "$FILENAME already exists. Skipping."
    return
  fi

  aws-vault exec prod-power -- ${IDP_ROOT}/bin/query-cloudwatch \
      --complete \
      --from "2023-08-${DAY}T00:00:00Z" \
      --to "2023-08-${DAY}T23:59:59.99999Z" \
      --app idp --env prod --log events.log \
      --json \
      <<QUERY | gzip >  results_${DAY}.ndjson.gz
  filter
      properties.user_id != 'anonymous-uuid' and
      !(strcontains(name, 'User Registration:')) and
      !strcontains(name, 'Telephony:') and
      !strcontains(name, 'Multi-Factor Authentication') and
      !strcontains(name, 'SAML Auth') and
      !strcontains(name, 'Sign in page visited') and
      !strcontains(name, 'OpenID Connect:') and
      !strcontains(name, 'Email Sent') and
      !strcontains(name, 'Email and Password Authentication') and
      !strcontains(name, 'User marked authenticated') and
      !strcontains(name, 'OTP:') and
      !strcontains(name, 'TOTP') and
      !strcontains(name, 'OIDC') and
      !strcontains(name, 'GetUspsProofingResultsJob') and
      !strcontains(name, 'Backup Code') and
      !strcontains(name, 'User registration:') and
      !strcontains(name, 'Password Creation') and
      !strcontains(name, 'Authentication Confirmation') and
      !strcontains(name, 'Password Reset:') and
      !strcontains(name, 'Account Reset:') and
      !strcontains(name, 'Add Email:') and
      name not in [
          'Account deletion and reset visited',
          'Logout Initiated'
          'PIV/CAC Login',
          'Remembered device used for authentication',
          'Rules of Use Submitted',
          'Rules of Use Visited',
          'Show Password Button Clicked',
          'SP redirect initiated',
          'User 2FA Reauthentication Required',
          'WebAuthn Setup Visited',
          'Remote Logout completed',
          'Phone Setup Visited',
          'Frontend: IdV: Acuant SDK loaded'
      ]
  | fields
      @timestamp,
      name,
      properties.user_id as user_id,
      properties.service_provider as service_provider,
      properties.locale as locale,
      properties.event_properties.proofing_components.document_type as document_type,
      properties.event_properties.flow_path as flow_path,
      properties.event_properties.getting_started_ab_test_bucket as bucket,
      properties.event_properties.session_duration as session_duration,
      properties.event_properties.redirect_url as redirect_url,
      properties.event_properties.success as success,
      properties.event_properties.fraud_review_pending as fraud_review_pending,
      properties.event_properties.fraud_rejection as fraud_rejection,
      properties.event_properties.gpo_verification_pending as gpo_verification_pending,
      properties.event_properties.in_person_verification_pending as in_person_verification_pending,
      properties.event_properties.deactivation_reason as deactivation_reason,
      properties.new_event as new_event,
      properties.browser_bot as browser_bot,
      properties.browser_device_name as browser_device_name,
      properties.browser_mobile as browser_mobile,
      properties.browser_name as browser_name,
      properties.browser_platform_name as browser_platform_name,
      properties.browser_platform_version as browser_platform_version,
      properties.browser_version as browser_version,
      properties.event_properties.proofing_components.threatmetrix_review_status as threatmetrix_review_status,
      properties.event_properties.DocAuthResult as doc_auth_result,
      properties.event_properties.DocClass as doc_class,
      properties.event_properties.DocClassCode as doc_class_code,
      properties.event_properties.DocClassName as doc_class_name,
      properties.event_properties.DocIsGeneric as doc_is_generic,
      properties.event_properties.DocIssue as doc_issue,
      properties.event_properties.DocIssueType as doc_issue_type,
      properties.event_properties.DocIssuerCode as doc_issuer_code,
      properties.event_properties.DocIssuerName as doc_issuer_name,
      properties.event_properties.DocIssuerType as doc_issuer_type
  | limit 10000
QUERY
}

get_day '01'
get_day '02'
get_day '03'
get_day '04'
get_day '05'
get_day '06'
get_day '07'
get_day '08'
