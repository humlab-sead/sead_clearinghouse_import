
##  SEAD Clearinghouse Import Log

### 2023-12 Dendrochronology

- [x] Move Excel file to data/input
- [x] Fix incorrect column name `tbl_dendro_date_notes.dendro_date_note_id` to `dendro_date_id`
- [x] Generate new staging database using SEAD Change Control System
```bash
[sead_change_control] ./bin/deploy-staging --create-database --on-conflict drop --source-type empty --target-db-name DBNAME --deploy-to-tag @YYYY.MM --ignore-git-tags
```
- [x] Load Excel into staging database using SEAD Clearing House Import System
```bash
[sead_clearinghouse_import] λ PYTHONPATH=. python importer/scripts/import_excel.py data/output/building_dendro_2023-12_import_v3.xlsx --no-timestamp --database DBNAME --register --explode --data-types dendrochronology --transfer-format csv
```
- [x] Commit submission by creating a change request in SEAD Change Control System
```bash
[sead_change_control] λ bin/commit-submission --id 5 --database DBNAME --project dendrochronology
```

### Optional (generate XML, then stop)
- [x] Generate XML file only
```bash
[sead_change_control] λ PYTHONPATH=. python importer/scripts/import_excel.py data/input/building_dendro_2023-12_import_v3.xlsx --no-timestamp --no-register --no-explode --data-types dendrochronology
```
  