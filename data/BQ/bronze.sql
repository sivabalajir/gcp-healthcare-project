-- Description: Create external tables for bronze dataset in BigQuery
-- Updated with new project ID & bucket name

CREATE EXTERNAL TABLE IF NOT EXISTS `long-nation-476207-n7.bronze_dataset.departments_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket021125/landing/hospital-a/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `long-nation-476207-n7.bronze_dataset.encounters_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket021125/landing/hospital-a/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `long-nation-476207-n7.bronze_dataset.patients_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket021125/landing/hospital-a/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `long-nation-476207-n7.bronze_dataset.providers_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket021125/landing/hospital-a/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `long-nation-476207-n7.bronze_dataset.transactions_ha` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket021125/landing/hospital-a/transactions/*.json']
);

---------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS `long-nation-476207-n7.bronze_dataset.departments_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket021125/landing/hospital-b/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `long-nation-476207-n7.bronze_dataset.encounters_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket021125/landing/hospital-b/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `long-nation-476207-n7.bronze_dataset.patients_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket021125/landing/hospital-b/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `long-nation-476207-n7.bronze_dataset.providers_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket021125/landing/hospital-b/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `long-nation-476207-n7.bronze_dataset.transactions_hb` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket021125/landing/hospital-b/transactions/*.json']
);

---------------------------------------------------------------------------------------------------------------------------
