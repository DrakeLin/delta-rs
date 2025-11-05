use std::fmt::Debug;
use std::sync::Arc;

use crate::kernel::schema::cast_record_batch;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{not_impl_err, ColumnStatistics, Result};
use datafusion::datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper};

/// Convert Utf8/Binary in a schema to LargeUtf8/LargeBinary to match file schema
fn adapt_schema_for_large_types(target_schema: &Schema, file_schema: &Schema) -> Schema {
    let fields: Vec<Arc<Field>> = target_schema
        .fields()
        .iter()
        .map(|field| {
            // Check if the file schema has this field as a large type
            let file_field = file_schema.field_with_name(field.name()).ok();
            let new_data_type = match (field.data_type(), file_field.map(|f| f.data_type())) {
                (DataType::Utf8, Some(DataType::LargeUtf8)) => DataType::LargeUtf8,
                (DataType::Binary, Some(DataType::LargeBinary)) => DataType::LargeBinary,
                (dt, _) => dt.clone(),
            };

            Arc::new(
                Field::new(field.name(), new_data_type, field.is_nullable())
                    .with_metadata(field.metadata().clone()),
            )
        })
        .collect();

    Schema::new(fields).with_metadata(target_schema.metadata().clone())
}

/// A Schema Adapter Factory which provides casting record batches from parquet to meet
/// delta lake conventions.
#[derive(Debug)]
pub(crate) struct DeltaSchemaAdapterFactory {}

impl SchemaAdapterFactory for DeltaSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(DeltaSchemaAdapter {
            projected_table_schema,
            table_schema,
        })
    }
}

pub(crate) struct DeltaSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output (projected) by
    /// the mapping.
    projected_table_schema: SchemaRef,
    /// Schema for the table
    table_schema: SchemaRef,
}

impl SchemaAdapter for DeltaSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.table_schema.field(index);
        Some(file_schema.fields.find(field.name())?.0)
    }

    fn map_schema(&self, file_schema: &Schema) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if self
                .projected_table_schema
                .fields()
                .find(file_field.name())
                .is_some()
            {
                projection.push(file_idx);
            }
        }

        // Adapt the projected schema to use large types if the file schema has them
        // This prevents overflow when casting LargeUtf8 back to Utf8
        let adapted_schema =
            adapt_schema_for_large_types(&self.projected_table_schema, file_schema);

        Ok((
            Arc::new(SchemaMapping {
                projected_schema: Arc::new(adapted_schema),
            }),
            projection,
        ))
    }
}

#[derive(Debug)]
pub(crate) struct SchemaMapping {
    projected_schema: SchemaRef,
}

impl SchemaMapper for SchemaMapping {
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let record_batch = cast_record_batch(&batch, self.projected_schema.clone(), false, true)?;
        Ok(record_batch)
    }

    fn map_column_statistics(
        &self,
        _file_col_statistics: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>> {
        not_impl_err!("Mapping column statistics is not implemented for DeltaSchemaAdapter")
    }
}
