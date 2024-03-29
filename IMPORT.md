
##  SEAD Clearinghouse Import Log

### 2023-12 Dendrochronology

- [x] Move Excel file to data/input
- [x] Fix incorrect column name `tbl_dendro_date_notes.dendro_date_note_id` to `dendro_date_id`
- [x] Generate new staging database using SEAD Change Control System
```bash
[sead_change_control] ./bin/deploy-staging --create-database --on-conflict drop --source-type empty --target-db-name sead_staging_202212 --deploy-to-tag @2022.12 --ignore-git-tags
```
- [x] Load Excel into staging database using SEAD Clearing House Import System
```bash
[sead_clearinghouse_import] λ PYTHONPATH=. python importer/scripts/import_excel.py data/output/building_dendro_2023-12_import_v3.xlsx --no-timestamp --database sead_staging_202212 --register --explode --data-types dendrochronology --transfer-format csv
```
- [x] Commit submission by creating a change request in SEAD Change Control System
```bash
[sead_change_control] λ bin/add-submission-change-request --id 5 --database sead_staging_202212 --project dendrochronology
```

### Optional (generate XML, then stop)
- [x] Generate XML file only
```bash
[sead_change_control] λ PYTHONPATH=. python importer/scripts/import_excel.py data/input/building_dendro_2023-12_import_v3.xlsx --no-timestamp --no-register --no-explode --data-types dendrochronology
```
  