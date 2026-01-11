# CSV Dispatcher Implementation

## Overview

The CSV dispatcher provides an alternative to XML for data submission processing. Instead of the previous two-step process (Submission → XML → CSV), the CSV dispatcher creates CSV files directly from the Submission object.

## Workflow Comparison

### Previous (XML-based):
```
Excel → Submission → XmlProcessor.dispatch → XML → xml_to_csv → CSV files
```

### New (CSV-based):
```
Excel → Submission → CsvProcessor.dispatch → CSV files
```

## Usage

To use the CSV dispatcher, specify `transfer_format: csv` in your configuration file or via CLI:

### Configuration File
```yaml
options:
  transfer_format: csv
```

### Command Line
```bash
python importer/scripts/import_excel.py \
  config.yml data.xlsx \
  --name "submission_name" \
  --transfer-format csv \
  --output-folder output/
```

## Output Files

The CSV dispatcher creates 4 tab-separated CSV files with the same format as the xml_to_csv conversion:

1. **`{basename}_tables.csv`**: Table metadata
   - Columns: `table_type`, `record_count`
   - Example: `tbl_sites	2`

2. **`{basename}_columns.csv`**: Column definitions
   - Columns: `table_type`, `column_name`, `column_type`
   - Example: `tbl_sites	site_name	character varying`

3. **`{basename}_records.csv`**: Record identifiers
   - Columns: `class_name`, `system_id`, `public_id`
   - Example: `Site	1	NULL`

4. **`{basename}_recordvalues.csv`**: Column values and FK relationships
   - Columns: `class_name`, `system_id`, `public_id`, `column_name`, `column_type`, `fk_system_id`, `fk_public_id`, `column_value`
   - Example: `Site	1	NULL	site_name	character varying	NULL	NULL	"Test Site"`

## Implementation Details

### File Location
- **Implementation**: `importer/dispatchers/to_csv.py`
- **Tests**: `tests/unit/test_csv_dispatcher.py`

### Key Components

#### CsvProcessor Class
- Implements `IDispatcher` interface
- Main method: `dispatch(schema, submission, table_names=None)`
- Output: 4 tab-separated CSV files

#### Processing Flow
1. Initialize output data structures (lists for tables, columns, records, recordvalues)
2. Iterate through each table in submission
3. For each table:
   - Collect table metadata (name, record count)
   - Collect column definitions
   - For each row:
     - Create record entry (system_id, public_id)
     - For each column:
       - Process foreign keys (resolve system_id → public_id)
       - Process regular columns
       - Store in recordvalues
4. Write all data to 4 CSV files

#### Foreign Key Resolution
- FK columns reference other tables using system_id during submission
- CsvProcessor resolves FK references to public_id by looking up the referenced table
- Example: `dataset_id=100` (system_id) → looks up tbl_datasets[100] → resolves to public_id=1
- Output includes both: `fk_system_id=100`, `fk_public_id=1`

### Value Formatting
- **NULL values**: Represented as string "NULL"
- **Integers**: Converted to string representation
- **Strings**: Quoted with double-quotes, internal quotes escaped (`"` → `""`)
- **Floats**: Converted to string representation

## Testing

Run the CSV dispatcher tests:
```bash
# Run CSV dispatcher tests only
pytest tests/unit/test_csv_dispatcher.py -v

# Run all unit tests
pytest tests/unit/ -v
```

Test coverage includes:
- Basic CSV file creation (4 files)
- Foreign key relationship handling
- Format compatibility with xml_to_csv output
- Column type handling
- NULL value handling

## Benefits

1. **Performance**: Eliminates XML intermediate step
2. **Simplicity**: Direct conversion from Submission to CSV
3. **Compatibility**: Output format identical to xml_to_csv
4. **Flexibility**: Users can choose XML or CSV based on needs

## Backward Compatibility

- The XML dispatcher remains the default
- Existing workflows continue to work unchanged
- CSV dispatcher is opt-in via configuration
