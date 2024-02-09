# fetchenergystats
Simple little helper scripts which can be used to fetch your energy info from various providers (Octopus, Givenergy, etc) and save them in formats like csv or parquet

## Limitations
Givenergy only allows you to fetch 15 days of stats at one time otherwise it returns no data at all. I have no intention, as of yet, to 'work around' this as I don't
want Givenergy blocking their API because people start to download everything all at once and max out their API.

## Notes
You must store your credentials in creds.yaml (insert them between the single quotes):

givenergy:
  token: ''

octopus:
  api_key: ''
  mpan: ''
  meter_serial: ''
