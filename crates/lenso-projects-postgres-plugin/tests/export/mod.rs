use super::*;
use futures::{executor::block_on, future::ready};
use lenso_capability_data_export_source::{CollectExportRequest, CollectExportResponse};
use std::cell::Cell;

fn request() -> CollectExportRequest {
    CollectExportRequest {
        export_id: "export_1".to_owned(),
        scope_kind: "organization".to_owned(),
        scope_id: "org_acme".to_owned(),
        subject: "usr_alice".to_owned(),
    }
}

fn assert_exhausted<T: fmt::Debug>(result: Result<T, StorageError>) {
    match result.unwrap_err() {
        StorageError::Runtime(RuntimeFailure::ResourceExhausted {
            capability,
            operation,
        }) => {
            assert_eq!(
                capability,
                lenso_capability_data_export_source::CAPABILITY_ID
            );
            assert_eq!(
                operation,
                lenso_capability_data_export_source::COLLECT_EXPORT_OPERATION
            );
        }
        error => panic!("expected ResourceExhausted, got {error:?}"),
    }
}

fn fixture(section: ExportSection, index: i64) -> Value {
    // IDs deliberately run opposite to insertion order for sequence-keyed arrays.
    let id = format!("id_{:04}", 1000 - index);
    let timestamp = "2026-09-15T00:00:00Z";
    match section {
        ExportSection::Comments => {
            json!({"comment_id": id, "issue_id": "issue_1", "body": "雪\n\"\\\u{0001}", "deleted": index % 2 == 0, "created_at": timestamp, "updated_at": timestamp})
        }
        ExportSection::ProjectUpdates => {
            json!({"update_id": id, "project_id": "project_1", "body": "hello", "health": "on_track", "created_at": timestamp})
        }
        ExportSection::Activity => {
            json!({"activity_id": (index * 3).to_string(), "project_id": null, "issue_id": null, "operation": "projects.write", "entity_kind": "issue", "entity_id": id, "revision": if index % 2 == 0 { Some("2") } else { None }, "occurred_at": timestamp})
        }
        ExportSection::AssignedIssueIds => json!(format!("issue_{index:04}")),
    }
}

fn synthetic_export(count: i64, max_bytes: usize) -> CollectExportResponse {
    block_on(collect_export_pages(
        &request(),
        max_bytes,
        |section, after| {
            let start = if section == ExportSection::AssignedIssueIds {
                after.issue_id.map_or(0, |id| {
                    id.strip_prefix("issue_").unwrap().parse::<i64>().unwrap()
                })
            } else {
                after.sequence.unwrap_or(0) / 3
            };
            ready(Ok(((start + 1)..=count)
                .take(usize::try_from(EXPORT_PAGE_SIZE).unwrap())
                .map(|index| {
                    let cursor = if section == ExportSection::AssignedIssueIds {
                        ExportCursor {
                            issue_id: Some(format!("issue_{index:04}")),
                            ..ExportCursor::default()
                        }
                    } else {
                        ExportCursor {
                            sequence: Some(index * 3),
                            ..ExportCursor::default()
                        }
                    };
                    (cursor, fixture(section, index))
                })
                .collect()))
        },
        |_, row| Ok(row),
    ))
    .unwrap()
}

#[test]
fn multiple_pages_preserve_exact_schema_and_array_order() {
    // Exercise short final pages and an exact multiple requiring an empty final fetch.
    for count in [EXPORT_PAGE_SIZE * 2, EXPORT_PAGE_SIZE * 2 + 3] {
        let result = synthetic_export(count, crate::MAX_EXPORT_BYTES);
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].item_name, "projects.json");
        assert_eq!(result.items[0].media_type, "application/json");
        let mut expected =
            json!({"organization_id": request().scope_id, "subject": request().subject});
        for section in ExportSection::ALL {
            expected[section.name()] =
                Value::Array((1..=count).map(|index| fixture(section, index)).collect());
        }
        assert_eq!(
            result.items[0].payload,
            serde_json::to_string(&expected).unwrap()
        );
        assert_eq!(result, synthetic_export(count, crate::MAX_EXPORT_BYTES));
    }
}

#[test]
fn empty_sections_and_exact_payload_boundary() {
    let result = synthetic_export(0, crate::MAX_EXPORT_BYTES);
    let payload = &result.items[0].payload;
    assert_eq!(
        payload,
        r#"{"organization_id":"org_acme","subject":"usr_alice","comments":[],"project_updates":[],"activity":[],"assigned_issue_ids":[]}"#
    );
    assert_eq!(result, synthetic_export(0, payload.len()));
    assert_exhausted(block_on(collect_export_pages(
        &request(),
        payload.len() - 1,
        |_, _| ready(Ok(Vec::<(ExportCursor, Value)>::new())),
        |_, row| Ok(row),
    )));
}

#[test]
fn protocol_maximum_boundary_counts_utf8_and_json_escaping() {
    let empty_len = synthetic_export(0, crate::MAX_EXPORT_BYTES).items[0]
        .payload
        .len();
    let escaped = "雪\n\"\\\u{0001}";
    let escaped_len = serde_json::to_string(escaped).unwrap().len();
    let value = format!(
        "{escaped}{}",
        "x".repeat(crate::MAX_EXPORT_BYTES - empty_len - escaped_len)
    );
    for extra in ["", "x"] {
        let result = block_on(collect_export_pages(
            &request(),
            crate::MAX_EXPORT_BYTES,
            |section, _| {
                ready(Ok(if section == ExportSection::AssignedIssueIds {
                    vec![(ExportCursor::default(), json!(format!("{value}{extra}")))]
                } else {
                    vec![]
                }))
            },
            |_, row| Ok(row),
        ));
        if extra.is_empty() {
            let result = result.unwrap();
            assert_eq!(result.items[0].payload.len(), crate::MAX_EXPORT_BYTES);
            let parsed: Value = serde_json::from_str(&result.items[0].payload).unwrap();
            assert_eq!(parsed["assigned_issue_ids"], json!([value]));
        } else {
            assert_exhausted(result);
        }
    }
}

#[test]
fn exhaustion_stops_decoding_and_loading_later_pages_and_sections() {
    let fetched = Cell::new(0);
    let decoded = Cell::new(0);
    // Exactly one full page fits; the first row on the second page exceeds the budget.
    let max_bytes = b"{\"organization_id\":\"org_acme\",\"subject\":\"usr_alice\",\"comments\":["
        .len()
        + usize::try_from(EXPORT_PAGE_SIZE).unwrap() * 3;
    let result = block_on(collect_export_pages(
        &request(),
        max_bytes,
        |section, after| {
            assert_eq!(section, ExportSection::Comments);
            fetched.set(fetched.get() + 1);
            let start = after.sequence.unwrap_or(0);
            ready(Ok(
                (start + 1..=start + EXPORT_PAGE_SIZE).collect::<Vec<_>>()
            ))
        },
        |_, index| {
            decoded.set(decoded.get() + 1);
            Ok((
                ExportCursor {
                    sequence: Some(index),
                    ..ExportCursor::default()
                },
                json!(10),
            ))
        },
    ));
    assert_exhausted(result);
    assert_eq!(fetched.get(), 2);
    assert_eq!(decoded.get(), EXPORT_PAGE_SIZE + 1);
}

#[test]
fn very_small_budget_stops_before_database_fetch() {
    assert_exhausted(block_on(collect_export_pages(
        &request(),
        1,
        |_, _| {
            panic!("no page should be fetched");
            #[allow(unreachable_code)]
            ready(Ok(Vec::<(ExportCursor, Value)>::new()))
        },
        |_, row| Ok(row),
    )));
}

#[test]
fn writer_never_grows_past_budget_even_with_large_escaped_value() {
    for limit in [1, 9, 64, 127] {
        let mut writer = ExportWriter::new(limit);
        assert_exhausted(writer.json(&"雪\n\"\\".repeat(1000)));
        assert!(writer.bytes.len() <= limit);
        assert!(writer.bytes.capacity() <= limit);
        let len = writer.bytes.len();
        assert_exhausted(writer.raw(b"x"));
        assert_eq!(writer.bytes.len(), len);
    }
}

#[cfg(feature = "postgres-acceptance")]
mod postgres;
