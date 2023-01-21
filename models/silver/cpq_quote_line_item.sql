{{ config(
  materialized='table',
  file_format='delta'
) }}

select CurrencyCode, DateCreated, DateModified, DistributionChannel, Division, EffectiveDate, ErrorMessage, ExternalId, ExternalSystemId, IsPrimary, MarketCode, MarketId, OpportunityId, OpportunityName, Origin, OwnerId, PriceBookId, QuoteId, QuoteNumber, RevisionNumber, StatusId, StatusName from bronze.cpq_quote_header;