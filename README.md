# fetchenergystats
Simple little helper scripts which can be used to fetch your energy info from various providers (Octopus, Givenergy, etc) and save them in formats like csv or parquet

You must store your credentials in creds.yaml (insert them between the single quotes):

givenergy:
  token: ''

octopus:
  api_key: ''
  mpan: ''
  meter_serial: ''
