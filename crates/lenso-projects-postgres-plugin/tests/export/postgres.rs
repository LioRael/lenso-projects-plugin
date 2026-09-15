use super::*;

// Use connection-local temporary tables to exercise the production queries and
// decoder, including rows from other organizations and subjects between cursor keys.
#[tokio::test]
async fn postgres_keyset_pages_preserve_filtering_fields_and_order() {
    let Ok(database_url) = std::env::var("LENSO_PROJECTS_TEST_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL acceptance; LENSO_PROJECTS_TEST_DATABASE_URL is unset");
        return;
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

    let request = request();
    let fetched = Cell::new(0);
    let result = collect_export_pages(
        &request,
        crate::MAX_EXPORT_BYTES,
        |section, after| {
            fetched.set(fetched.get() + 1);
            fetch_export_page(&pool, &request, section, after)
        },
        decode_export_row,
    )
    .await
    .unwrap();
    assert_eq!(fetched.get(), 12, "three bounded pages per section");
    assert_eq!(result, synthetic_export(131, crate::MAX_EXPORT_BYTES));
    let payload_len = result.items[0].payload.len();
    let exact = collect_export_pages(
        &request,
        payload_len,
        |section, after| fetch_export_page(&pool, &request, section, after),
        decode_export_row,
    )
    .await
    .unwrap();
    assert_eq!(exact, result);
    assert_exhausted(
        collect_export_pages(
            &request,
            payload_len - 1,
            |section, after| fetch_export_page(&pool, &request, section, after),
            decode_export_row,
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
        |section, after| fetch_export_page(&pool, &missing, section, after),
        decode_export_row,
    )
    .await
    .unwrap();
    let parsed: Value = serde_json::from_str(&empty.items[0].payload).unwrap();
    for section in ExportSection::ALL {
        assert_eq!(parsed[section.name()], json!([]));
    }
    pool.close().await;
}
