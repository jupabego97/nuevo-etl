# ETL productivo Alegra → PostgreSQL

Repositorio nuevo y aislado del legacy `cron-job`. Implementa un ETL robusto contra la API de Alegra con:

- Esquema PostgreSQL dedicado `alegra_etl` (no modifica `public`)
- Paginación correcta por `start/limit` y `metadata.total`
- Carga idempotente con UPSERT
- Raw JSONB auditable
- Webhooks durables + cron nocturno
- Reconciliación por fechas
- Marts analíticos `gold_sales_30d` y `gold_replenishment`
- Controles de calidad y observabilidad

## Requisitos

- Python 3.12+
- PostgreSQL (Railway o local)
- Credenciales Alegra vía variables de entorno

## Configuración

```bash
cp .env.example .env
pip install -e ".[dev]"
```

Variables mínimas:

- `DATABASE_URL`
- `ALEGRA_EMAIL` + `ALEGRA_TOKEN` **o** `ALEGRA_API_KEY`
- `WEBHOOK_SECRET`
- `DB_SCHEMA=alegra_etl`

## Comandos

```bash
alegra-etl bootstrap
alegra-etl migrate
alegra-etl backfill
alegra-etl daily-sync
alegra-etl reconcile --resource invoices --days 30
alegra-etl process-webhooks
alegra-etl build-marts
alegra-etl serve-webhooks
```

## Arquitectura

1. **Extract**: cliente HTTP con rate limit (150 req/min) y reintentos
2. **Raw**: guarda cada página/consulta en `raw_documents`
3. **Transform/Load**: parsers normalizados → dimensiones y hechos
4. **Quality**: checks de duplicados, huérfanos y voids
5. **Gold**: marts retail con joins por `item_id`, no por nombre
6. **Webhooks**: FastAPI recibe eventos, persiste y reprocesa consultando Alegra

## Railway

Despliega **dos servicios** desde este repo:

| Servicio | Config | Comando |
|----------|--------|---------|
| Webhooks | `railway.json` | `uvicorn alegra_etl.web.app:app --host 0.0.0.0 --port $PORT` |
| Cron ETL | `railway-cron.json` | `alegra-etl daily-sync` |

Horario recomendado del cron:

- Colombia (UTC-5): 02:00
- UTC equivalente: **07:00**

Configura en Alegra webhooks hacia:

```text
POST https://<tu-servicio>.up.railway.app/webhooks/alegra
Header: X-Webhook-Secret: <WEBHOOK_SECRET>
```

Eventos soportados:

- `new-invoice`, `edit-invoice`, `delete-invoice`
- `new-bill`, `edit-bill`, `delete-bill`
- `new-client`, `edit-client`, `delete-client`
- `new-item`, `edit-item`, `delete-item`

## Esquema principal

### Control
- `etl_runs`, `etl_stage_runs`, `sync_checkpoints`
- `raw_documents`, `webhook_events`, `dead_letter_events`
- `quality_check_results`

### Dimensiones
- `dim_item`, `dim_item_price`, `dim_item_inventory`
- `dim_contact`, `dim_seller`, `dim_warehouse`
- `dim_tax`, `dim_currency`, `dim_cost_center`, `dim_company`

### Hechos
- Ventas: `fact_sales_invoice`, `fact_sales_invoice_line`
- Pagos: `fact_income_payment`, `fact_income_payment_application`
- Devoluciones: `fact_credit_note`, `fact_credit_note_line`
- Compras: `fact_purchase_bill`, `fact_purchase_bill_line`
- Inventario/finanzas: ajustes, transferencias, bancos

### Gold
- `gold_sales_30d`
- `gold_replenishment`
- `replenishment_policy`

## Seguridad

- No hay credenciales hardcodeadas
- Logs con redacción de secretos
- Webhook protegido por header secreto
- Rotar credenciales expuestas en repos anteriores antes de producción

## Base de datos: nueva vs misma instancia

El ETL **no depende** de tablas legacy en `public`. Solo necesita un `DATABASE_URL` válido y crea sus objetos bajo el esquema `alegra_etl` (configurable con `DB_SCHEMA`).

### Opción A — PostgreSQL nueva (recomendada para producción limpia)

Usa un servicio Postgres dedicado en Railway (u otro proveedor) distinto al de `cron-job`.

**Ventajas**
- Aislamiento total: cero riesgo sobre `facturas`, `items`, `reportes_ventas_30dias`, etc.
- Backups, permisos, restores y escalado independientes.
- Corte limpio: el legacy puede apagarse cuando valides el nuevo ETL.

**Consideraciones**
- Requiere **backfill completo** desde Alegra antes de usar reportes.
- Segundo Postgres en Railway (costo adicional).
- Los consumidores (dashboards, apps) deben repuntar al nuevo `DATABASE_URL`.

**Configuración**

```env
DATABASE_URL=postgresql+psycopg://user:pass@host:5432/nuevo_etl_prod
DB_SCHEMA=alegra_etl
```

### Opción B — Misma PostgreSQL, esquema aislado (recomendada para transición)

Comparte la instancia actual pero **nunca escribe en `public`**. Alembic y el loader operan solo en `alegra_etl`.

**Ventajas**
- Comparación SQL directa entre legacy (`public`) y nuevo (`alegra_etl`) en una sola conexión.
- Un solo Postgres en Railway.
- Migración gradual de dashboards sin duplicar infraestructura.

**Consideraciones**
- Comparten CPU, conexiones y disco con el cron viejo.
- Hay que vigilar que ningún consumidor legacy se vea afectado por carga del backfill.

**Configuración**

```env
DATABASE_URL=postgresql+psycopg://user:pass@host:5432/railway
DB_SCHEMA=alegra_etl
```

### Decisión recomendada

| Objetivo | Opción |
|----------|--------|
| Reemplazar `cron-job` y arrancar limpio | **A — BD nueva** |
| Validar métricas lado a lado antes de migrar | **B — Misma BD** |

En ambos casos el código, migraciones y comandos CLI son idénticos.

## Transición desde cron-job

### Con BD nueva (corte limpio)

1. Crear Postgres dedicado y configurar `DATABASE_URL` en Railway.
2. Ejecutar `bootstrap` → `migrate` → `backfill`.
3. Validar con `reconcile` y revisar `gold_sales_30d`, `gold_replenishment`.
4. Repuntar dashboards/consumidores al nuevo `DATABASE_URL`.
5. Desactivar el cron legacy cuando los conteos coincidan con Alegra.

### Con misma BD (convivencia temporal)

1. Mantener el `DATABASE_URL` actual; confirmar `DB_SCHEMA=alegra_etl`.
2. Ejecutar `backfill` en `nuevo-etl` (no altera tablas de `public`).
3. Comparar en SQL: `public.reportes_ventas_30dias` vs `alegra_etl.gold_sales_30d`.
4. Migrar consumidores al esquema `alegra_etl` cuando estés conforme.
5. Mantener `cron-job` intacto hasta completar la validación.

**En ambos escenarios:** rotar credenciales expuestas en repos anteriores antes de producción.

## Pruebas

```bash
pytest -q
ruff check src tests
```

## Recursos Alegra cubiertos

Maestros, ventas, compras, inventario, contabilidad, bancos y configuración según flags:

- `items`, `contacts`, `sellers`, `warehouses`, `price-lists`, `taxes`, `currencies`
- `invoices`, `payments`, `credit-notes`, `estimates`, `remissions`
- `bills`, `purchase-orders`, `debit-notes`
- `inventory-adjustments`, `warehouse-transfers`
- `categories`, `cost-centers`, `journals`, `bank-accounts`, `conciliations`

Recursos opcionales por plan/país quedan detrás de feature flags.
