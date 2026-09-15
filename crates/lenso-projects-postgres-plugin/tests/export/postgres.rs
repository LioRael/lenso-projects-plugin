use super::*;

// Use connection-local temporary tables to exercise the production queries and
// decoder, including rows from other organizations and subjects between cursor keys.
async fn fixture_pool() -> Option<PgPool> {
    let Ok(database_url) = std::env::var("LENSO_PROJECTS_TEST_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL acceptance; LENSO_PROJECTS_TEST_DATABASE_URL is unset");
        return None;
    };
    let database_name = database_url
        .split('?')
        .next()
        .and_then(|value| value.rsplit('/').next())
        .unwrap_or_default();
    assert!(database_name.starts_with("lenso_projects_test"));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::raw_sql(
        r"
        CREATE TEMP TABLE export_fixture AS
        SELECT i, variant, (i * 3 + variant)::bigint AS seq,
            CASE WHEN variant = 2 THEN 'org_other' ELSE 'org_acme' END AS org,
            CASE WHEN variant = 1 THEN 'usr_other' ELSE 'usr_alice' END AS subject,
            'id_' || lpad((1000 - i)::text, 4, '0') AS id,
            '2026-09-15T00:00:00Z'::timestamptz AS at
        FROM generate_series(1, 131) i CROSS JOIN generate_series(0, 2) variant;
        CREATE TEMP TABLE comments AS
        SELECT seq AS row_seq, org AS organization_id, subject AS author_subject,
            id AS comment_id, 'issue_1'::text AS issue_id, ''::text AS body,
            i % 2 = 0 AS deleted, at AS created_at, at AS updated_at
        FROM export_fixture ORDER BY seq DESC;
        CREATE TEMP TABLE project_updates AS
        SELECT seq AS row_seq, org AS organization_id, subject AS author_subject,
            id AS update_id, 'project_1'::text AS project_id, 'hello'::text AS body,
            'on_track'::text AS health, at AS created_at
        FROM export_fixture ORDER BY seq DESC;
        CREATE TEMP TABLE project_activity AS
        SELECT seq AS activity_id, org AS organization_id, subject AS actor_subject,
            NULL::text AS project_id, NULL::text AS issue_id, 'projects.write'::text AS operation,
            'issue'::text AS entity_kind, id AS entity_id,
            CASE WHEN i % 2 = 0 THEN 2 ELSE NULL END::bigint AS revision, at AS occurred_at
        FROM export_fixture ORDER BY seq DESC;
        CREATE TEMP TABLE issues AS
        SELECT 'issue_' || lpad(i::text, 4, '0') AS issue_id,
            org AS organization_id, subject AS assignee_subject
        FROM export_fixture ORDER BY seq DESC;
        ",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE comments SET body=$1")
        .bind("雪\n\"\\\u{0001}")
        .execute(&pool)
        .await
        .unwrap();

    Some(pool)
}

#[tokio::test]
async fn postgres_keyset_pages_preserve_filtering_fields_and_order() {
    let Some(pool) = fixture_pool().await else {
        return;
    };
    let request = request();
    let fetched = Cell::new(0);
    let result = collect_export_pages(
        &request,
        crate::MAX_EXPORT_BYTES,
        |section, after, remaining| {
            fetched.set(fetched.get() + 1);
            fetch_export_page(&pool, &request, section, after, remaining)
        },
        write_export_row,
    )
    .await
    .unwrap();
    assert_eq!(fetched.get(), 12, "three bounded pages per section");
    assert_eq!(result, synthetic_export(131, crate::MAX_EXPORT_BYTES));
    let payload_len = result.items[0].payload.len();
    let exact = collect_export_pages(
        &request,
        payload_len,
        |section, after, remaining| fetch_export_page(&pool, &request, section, after, remaining),
        write_export_row,
    )
    .await
    .unwrap();
    assert_eq!(exact, result);
    assert_exhausted(
        collect_export_pages(
            &request,
            payload_len - 1,
            |section, after, remaining| {
                fetch_export_page(&pool, &request, section, after, remaining)
            },
            write_export_row,
        )
        .await,
    );
    let missing = CollectExportRequest {
        subject: "usr_missing".to_owned(),
        ..request.clone()
    };
    let empty = collect_export_pages(
        &missing,
        crate::MAX_EXPORT_BYTES,
        |section, after, remaining| fetch_export_page(&pool, &missing, section, after, remaining),
        write_export_row,
    )
    .await
    .unwrap();
    let parsed: Value = serde_json::from_str(&empty.items[0].payload).unwrap();
    for section in ExportSection::ALL {
        assert_eq!(parsed[section.name()], json!([]));
    }
    pool.close().await;
}

// Inspect the SQLx wire value before decoding: the full stored body must never
// enter the client row buffer, even when the first oversized row is on page two.
#[tokio::test]
async fn postgres_oversized_bodies_are_null_on_wire_and_resource_exhausted() {
    use sqlx::ValueRef;

    let Some(pool) = fixture_pool().await else {
        return;
    };
    let request = request();
    for max_bytes in [262_144, crate::MAX_EXPORT_BYTES] {
        let limit = i32::try_from(max_bytes).unwrap();
        for section in [ExportSection::Comments, ExportSection::ProjectUpdates] {
            let (update, lengths, restore, body) = match section {
                ExportSection::Comments => (
                    "UPDATE comments SET body=repeat('雪', $1 / 3 + 1) WHERE row_seq=$2",
                    "SELECT octet_length(body),length(body) FROM comments WHERE row_seq=$1",
                    "UPDATE comments SET body=$1 WHERE row_seq=$2",
                    "雪\n\"\\\u{0001}",
                ),
                ExportSection::ProjectUpdates => (
                    "UPDATE project_updates SET body=repeat('雪', $1 / 3 + 1) WHERE row_seq=$2",
                    "SELECT octet_length(body),length(body) FROM project_updates WHERE row_seq=$1",
                    "UPDATE project_updates SET body=$1 WHERE row_seq=$2",
                    "hello",
                ),
                _ => unreachable!(),
            };
            for index in [1, EXPORT_PAGE_SIZE + 1] {
                let seq = index * 3;
                // Construct the oversized TEXT entirely on the server; only its
                // scalar lengths and the guarded NULL are read back into Rust.
                sqlx::query(update)
                    .bind(limit)
                    .bind(seq)
                    .execute(&pool)
                    .await
                    .unwrap();
                let (bytes, characters): (i32, i32) = sqlx::query_as(lengths)
                    .bind(seq)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                assert!(bytes > limit);
                assert!(characters < limit, "the guard must count UTF-8 bytes");
                let after = ExportCursor {
                    sequence: (index > 1).then_some((index - 1) * 3),
                    ..ExportCursor::default()
                };
                let mut rows =
                    fetch_export_page(&pool, &request, section, after, max_bytes).unwrap();
                let row = rows.try_next().await.unwrap().unwrap();
                assert_eq!(row.get::<i64, _>("row_seq"), seq);
                assert!(row.try_get_raw("body").unwrap().is_null());
                assert_exhausted(write_export_row(
                    section,
                    row,
                    &mut ExportWriter::new(max_bytes),
                ));
                let mut count = 1;
                while rows.try_next().await.unwrap().is_some() {
                    count += 1;
                }
                assert_eq!(count, EXPORT_PAGE_SIZE);
                drop(rows);

                let fetched = Cell::new(0);
                assert_exhausted(
                    collect_export_pages(
                        &request,
                        max_bytes,
                        |section, after, remaining| {
                            fetched.set(fetched.get() + 1);
                            fetch_export_page(&pool, &request, section, after, remaining)
                        },
                        write_export_row,
                    )
                    .await,
                );
                let preceding_pages = if section == ExportSection::ProjectUpdates {
                    3
                } else {
                    0
                };
                assert_eq!(
                    fetched.get(),
                    preceding_pages + if index == 1 { 1 } else { 2 }
                );
                sqlx::query(restore)
                    .bind(body)
                    .bind(seq)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        }
    }
    // Bodies outside the requested organization or subject must not exhaust this export.
    sqlx::raw_sql(
        "UPDATE comments SET body=repeat('x', 2097152) WHERE row_seq IN (4, 5);
         UPDATE project_updates SET body=repeat('x', 2097152) WHERE row_seq IN (4, 5);",
    )
    .execute(&pool)
    .await
    .unwrap();
    let result = collect_export_pages(
        &request,
        crate::MAX_EXPORT_BYTES,
        |section, after, remaining| fetch_export_page(&pool, &request, section, after, remaining),
        write_export_row,
    )
    .await
    .unwrap();
    assert_eq!(result, synthetic_export(131, crate::MAX_EXPORT_BYTES));
    pool.close().await;
}

#[tokio::test]
async fn postgres_body_guard_uses_remaining_page_budget() {
    use sqlx::ValueRef;
    use std::cell::RefCell;

    let Some(pool) = fixture_pool().await else {
        return;
    };
    let request = request();
    let max_bytes = 262_144;
    let budgets = RefCell::new(Vec::new());
    collect_export_pages(
        &request,
        max_bytes,
        |section, after, remaining| {
            budgets
                .borrow_mut()
                .push((section, after.sequence, remaining));
            fetch_export_page(&pool, &request, section, after, remaining)
        },
        write_export_row,
    )
    .await
    .unwrap();

    for section in [ExportSection::Comments, ExportSection::ProjectUpdates] {
        let update = match section {
            ExportSection::Comments => "UPDATE comments SET body=$1 WHERE row_seq=$2",
            ExportSection::ProjectUpdates => "UPDATE project_updates SET body=$1 WHERE row_seq=$2",
            _ => unreachable!(),
        };
        for index in [1, EXPORT_PAGE_SIZE + 1] {
            let after = (index > 1).then_some((index - 1) * 3);
            let remaining = budgets
                .borrow()
                .iter()
                .find(|(s, cursor, _)| *s == section && *cursor == after)
                .unwrap()
                .2;
            assert!(remaining + 1 < max_bytes);
            // Fits the configured ceiling, but cannot fit what is left at this seek.
            sqlx::query(update)
                .bind("x".repeat(remaining + 1))
                .bind(index * 3)
                .execute(&pool)
                .await
                .unwrap();
            let marked = Cell::new(false);
            assert_exhausted(
                collect_export_pages(
                    &request,
                    max_bytes,
                    |s, cursor, budget| fetch_export_page(&pool, &request, s, cursor, budget),
                    |s, row, writer| {
                        if s == section && row.get::<i64, _>("row_seq") == index * 3 {
                            assert!(row.try_get_raw("body").unwrap().is_null());
                            marked.set(true);
                        }
                        write_export_row(s, row, writer)
                    },
                )
                .await,
            );
            assert!(marked.get());
            let body = fixture(section, index)["body"].as_str().unwrap().to_owned();
            sqlx::query(update)
                .bind(body)
                .bind(index * 3)
                .execute(&pool)
                .await
                .unwrap();
        }
    }
    pool.close().await;
}

#[tokio::test]
async fn postgres_large_first_and_later_bodies_fit_exact_json_budget() {
    let Some(pool) = fixture_pool().await else {
        return;
    };
    let request = request();
    let max_bytes = 262_144;
    let baseline = synthetic_export(131, max_bytes);
    for section in [ExportSection::Comments, ExportSection::ProjectUpdates] {
        let update = match section {
            ExportSection::Comments => "UPDATE comments SET body=$1 WHERE row_seq=$2",
            ExportSection::ProjectUpdates => "UPDATE project_updates SET body=$1 WHERE row_seq=$2",
            _ => unreachable!(),
        };
        for index in [1, EXPORT_PAGE_SIZE + 1, EXPORT_PAGE_SIZE + 2] {
            let mut expected: Value = serde_json::from_str(&baseline.items[0].payload).unwrap();
            let body = &mut expected[section.name()][usize::try_from(index - 1).unwrap()]["body"];
            let original = body.as_str().unwrap().to_owned();
            let suffix = "雪\n\"\\\u{0001}";
            let padding = max_bytes - baseline.items[0].payload.len()
                + serde_json::to_string(&original).unwrap().len()
                - serde_json::to_string(suffix).unwrap().len();
            let large = format!("{}{suffix}", "x".repeat(padding));
            assert!(large.len() > max_bytes / 2, "no arbitrary per-row fraction");
            *body = json!(large);
            let expected = serde_json::to_string(&expected).unwrap();
            assert_eq!(expected.len(), max_bytes);
            sqlx::query(update)
                .bind(large)
                .bind(index * 3)
                .execute(&pool)
                .await
                .unwrap();
            let result = collect_export_pages(
                &request,
                max_bytes,
                |s, after, remaining| fetch_export_page(&pool, &request, s, after, remaining),
                write_export_row,
            )
            .await
            .unwrap();
            assert_eq!(result.items[0].payload, expected);
            assert_exhausted(
                collect_export_pages(
                    &request,
                    max_bytes - 1,
                    |s, after, remaining| fetch_export_page(&pool, &request, s, after, remaining),
                    write_export_row,
                )
                .await,
            );
            sqlx::query(update)
                .bind(original)
                .bind(index * 3)
                .execute(&pool)
                .await
                .unwrap();
        }
    }
    pool.close().await;
}
