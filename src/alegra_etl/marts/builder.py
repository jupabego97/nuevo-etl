"""Construcción de marts analíticos retail."""

from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

from alegra_etl.config import Settings

logger = logging.getLogger(__name__)

GOLD_SALES_30D_SQL = """
CREATE TABLE IF NOT EXISTS gold_sales_30d (
    id BIGSERIAL PRIMARY KEY,
    item_alegra_id VARCHAR(50),
    item_name VARCHAR(500) NOT NULL,
    familia VARCHAR(200),
    invoice_date DATE NOT NULL,
    quantity NUMERIC(20,6) NOT NULL,
    unit_price NUMERIC(20,6),
    line_total NUMERIC(20,6),
    payment_method VARCHAR(50),
    seller_name VARCHAR(300),
    avg_purchase_price NUMERIC(20,6),
    top_supplier VARCHAR(500),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""

GOLD_REPLENISHMENT_SQL = """
CREATE TABLE IF NOT EXISTS gold_replenishment (
    id BIGSERIAL PRIMARY KEY,
    item_alegra_id VARCHAR(50),
    item_name VARCHAR(500) NOT NULL,
    familia VARCHAR(200),
    warehouse_alegra_id VARCHAR(50) DEFAULT 'default',
    available_quantity NUMERIC(20,6),
    sales_7d NUMERIC(20,6),
    sales_15d NUMERIC(20,6),
    sales_30d NUMERIC(20,6),
    sales_60d NUMERIC(20,6),
    sales_90d NUMERIC(20,6),
    credit_returns_90d NUMERIC(20,6),
    net_demand_90d NUMERIC(20,6),
    avg_daily_demand NUMERIC(20,6),
    demand_trend_pct NUMERIC(10,4),
    lead_time_days INTEGER,
    safety_stock NUMERIC(20,6),
    reorder_point NUMERIC(20,6),
    suggested_order_qty NUMERIC(20,6),
    last_purchase_price NUMERIC(20,6),
    last_purchase_date DATE,
    top_supplier VARCHAR(500),
    avg_sale_price NUMERIC(20,6),
    margin_pct NUMERIC(10,4),
    markup_pct NUMERIC(10,4),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""


class MartBuilder:
    def __init__(self, settings: Settings, session: Session):
        self.settings = settings
        self.session = session
        self.schema = settings.db_schema

    def build_all(self) -> dict[str, int]:
        sales = self.build_sales_30d()
        replenishment = self.build_replenishment()
        return {"gold_sales_30d": sales, "gold_replenishment": replenishment}

    def build_sales_30d(self) -> int:
        self.session.execute(text(GOLD_SALES_30D_SQL))
        self.session.execute(text("TRUNCATE TABLE gold_sales_30d"))
        insert_sql = text(
            """
            INSERT INTO gold_sales_30d (
                item_alegra_id, item_name, familia, invoice_date, quantity,
                unit_price, line_total, payment_method, seller_name,
                avg_purchase_price, top_supplier
            )
            WITH sales AS (
                SELECT
                    l.item_alegra_id,
                    l.item_name,
                    i.family AS familia,
                    h.invoice_date,
                    l.quantity,
                    l.unit_price,
                    l.line_total,
                    h.payment_form AS payment_method,
                    h.seller_name
                FROM fact_sales_invoice_line l
                JOIN fact_sales_invoice h
                  ON h.company_id = l.company_id
                 AND h.alegra_id = l.invoice_alegra_id
                LEFT JOIN dim_item i
                  ON i.company_id = l.company_id
                 AND i.alegra_id = l.item_alegra_id
                WHERE l.company_id = :company_id
                  AND h.invoice_date >= CURRENT_DATE - INTERVAL '30 days'
                  AND COALESCE(h.status, '') <> 'void'
                  AND h.deleted_at IS NULL
            ),
            purchases AS (
                SELECT
                    pl.item_alegra_id,
                    AVG(pl.unit_price) AS avg_purchase_price,
                    MODE() WITHIN GROUP (ORDER BY pb.provider_name) AS top_supplier
                FROM fact_purchase_bill_line pl
                JOIN fact_purchase_bill pb
                  ON pb.company_id = pl.company_id
                 AND pb.alegra_id = pl.bill_alegra_id
                WHERE pl.company_id = :company_id
                  AND pl.line_kind = 'item'
                  AND pl.item_alegra_id IS NOT NULL
                  AND pb.bill_date >= CURRENT_DATE - INTERVAL '90 days'
                  AND pb.deleted_at IS NULL
                GROUP BY pl.item_alegra_id
            )
            SELECT
                s.item_alegra_id,
                s.item_name,
                s.familia,
                s.invoice_date,
                s.quantity,
                s.unit_price,
                s.line_total,
                s.payment_method,
                s.seller_name,
                p.avg_purchase_price,
                p.top_supplier
            FROM sales s
            LEFT JOIN purchases p ON p.item_alegra_id = s.item_alegra_id
            """
        )
        result = self.session.execute(insert_sql, {"company_id": self.settings.company_id})
        count = result.rowcount or 0
        logger.info("gold_sales_30d regenerada con %s filas", count)
        return count

    def build_replenishment(self) -> int:
        self.session.execute(text(GOLD_REPLENISHMENT_SQL))
        self.session.execute(text("TRUNCATE TABLE gold_replenishment"))
        insert_sql = text(
            """
            INSERT INTO gold_replenishment (
                item_alegra_id, item_name, familia, warehouse_alegra_id,
                available_quantity, sales_7d, sales_15d, sales_30d, sales_60d, sales_90d,
                credit_returns_90d, net_demand_90d, avg_daily_demand, demand_trend_pct,
                lead_time_days, safety_stock, reorder_point, suggested_order_qty,
                last_purchase_price, last_purchase_date, top_supplier,
                avg_sale_price, margin_pct, markup_pct
            )
            WITH item_base AS (
                SELECT
                    i.company_id,
                    i.alegra_id AS item_alegra_id,
                    i.name AS item_name,
                    i.family AS familia,
                    COALESCE(inv.available_quantity, 0) AS available_quantity,
                    COALESCE(inv.warehouse_alegra_id, 'default') AS warehouse_alegra_id
                FROM dim_item i
                LEFT JOIN dim_item_inventory inv
                  ON inv.company_id = i.company_id
                 AND inv.item_alegra_id = i.alegra_id
                WHERE i.company_id = :company_id
                  AND i.deleted_at IS NULL
            ),
            sales_stats AS (
                SELECT
                    l.item_alegra_id,
                    SUM(CASE WHEN h.invoice_date >= CURRENT_DATE - INTERVAL '7 days'
                             THEN l.quantity ELSE 0 END) AS sales_7d,
                    SUM(CASE WHEN h.invoice_date >= CURRENT_DATE - INTERVAL '15 days'
                             THEN l.quantity ELSE 0 END) AS sales_15d,
                    SUM(CASE WHEN h.invoice_date >= CURRENT_DATE - INTERVAL '30 days'
                             THEN l.quantity ELSE 0 END) AS sales_30d,
                    SUM(CASE WHEN h.invoice_date >= CURRENT_DATE - INTERVAL '60 days'
                             THEN l.quantity ELSE 0 END) AS sales_60d,
                    SUM(CASE WHEN h.invoice_date >= CURRENT_DATE - INTERVAL '90 days'
                             THEN l.quantity ELSE 0 END) AS sales_90d,
                    AVG(l.unit_price) FILTER (
                        WHERE h.invoice_date >= CURRENT_DATE - INTERVAL '30 days'
                    ) AS avg_sale_price
                FROM fact_sales_invoice_line l
                JOIN fact_sales_invoice h
                  ON h.company_id = l.company_id
                 AND h.alegra_id = l.invoice_alegra_id
                WHERE l.company_id = :company_id
                  AND COALESCE(h.status, '') <> 'void'
                  AND h.deleted_at IS NULL
                GROUP BY l.item_alegra_id
            ),
            credit_stats AS (
                SELECT
                    cn.item_alegra_id,
                    SUM(cn.quantity) AS returned_qty
                FROM fact_credit_note_line cn
                JOIN fact_credit_note h
                  ON h.company_id = cn.company_id
                 AND h.alegra_id = cn.credit_note_alegra_id
                WHERE cn.company_id = :company_id
                  AND h.note_date >= CURRENT_DATE - INTERVAL '90 days'
                GROUP BY cn.item_alegra_id
            ),
            purchase_stats AS (
                SELECT
                    pl.item_alegra_id,
                    MODE() WITHIN GROUP (ORDER BY pb.provider_name) AS top_supplier
                FROM fact_purchase_bill_line pl
                JOIN fact_purchase_bill pb
                  ON pb.company_id = pl.company_id
                 AND pb.alegra_id = pl.bill_alegra_id
                WHERE pl.company_id = :company_id
                  AND pl.line_kind = 'item'
                  AND pl.item_alegra_id IS NOT NULL
                  AND pb.deleted_at IS NULL
                GROUP BY pl.item_alegra_id
            ),
            last_purchase AS (
                SELECT DISTINCT ON (pl.item_alegra_id)
                    pl.item_alegra_id,
                    pl.unit_price AS last_purchase_price,
                    pb.bill_date AS last_purchase_date
                FROM fact_purchase_bill_line pl
                JOIN fact_purchase_bill pb
                  ON pb.company_id = pl.company_id
                 AND pb.alegra_id = pl.bill_alegra_id
                WHERE pl.company_id = :company_id
                  AND pl.line_kind = 'item'
                  AND pl.item_alegra_id IS NOT NULL
                  AND pb.deleted_at IS NULL
                ORDER BY pl.item_alegra_id, pb.bill_date DESC
            ),
            policy AS (
                SELECT *
                FROM replenishment_policy
                WHERE company_id = :company_id AND is_active = TRUE
            )
            SELECT
                b.item_alegra_id,
                b.item_name,
                b.familia,
                b.warehouse_alegra_id,
                b.available_quantity,
                COALESCE(s.sales_7d, 0),
                COALESCE(s.sales_15d, 0),
                COALESCE(s.sales_30d, 0),
                COALESCE(s.sales_60d, 0),
                COALESCE(s.sales_90d, 0),
                COALESCE(c.returned_qty, 0),
                COALESCE(s.sales_90d, 0) - COALESCE(c.returned_qty, 0),
                GREATEST(COALESCE(s.sales_30d, 0) - COALESCE(c.returned_qty, 0) * 0.33, 0) / 30.0,
                CASE
                    WHEN COALESCE(s.sales_30d, 0) > 0
                    THEN ((COALESCE(s.sales_7d, 0) / 7.0) - (COALESCE(s.sales_30d, 0) / 30.0))
                         / (COALESCE(s.sales_30d, 0) / 30.0) * 100
                    ELSE NULL
                END,
                COALESCE(p.lead_time_days, 7),
                (GREATEST(COALESCE(s.sales_30d, 0) - COALESCE(c.returned_qty, 0) * 0.33, 0) / 30.0)
                    * COALESCE(p.safety_stock_days, 3),
                (GREATEST(COALESCE(s.sales_30d, 0) - COALESCE(c.returned_qty, 0) * 0.33, 0) / 30.0)
                    * (COALESCE(p.lead_time_days, 7) + COALESCE(p.safety_stock_days, 3)),
                GREATEST(
                    (GREATEST(COALESCE(s.sales_30d, 0) - COALESCE(c.returned_qty, 0) * 0.33, 0) / 30.0)
                        * COALESCE(p.review_period_days, 30)
                    - b.available_quantity,
                    0
                ),
                lp.last_purchase_price,
                lp.last_purchase_date,
                ps.top_supplier,
                s.avg_sale_price,
                CASE
                    WHEN s.avg_sale_price > 0 AND lp.last_purchase_price IS NOT NULL
                    THEN ((s.avg_sale_price - lp.last_purchase_price) / s.avg_sale_price) * 100
                    ELSE NULL
                END,
                CASE
                    WHEN lp.last_purchase_price > 0 AND s.avg_sale_price IS NOT NULL
                    THEN ((s.avg_sale_price - lp.last_purchase_price) / lp.last_purchase_price) * 100
                    ELSE NULL
                END
            FROM item_base b
            LEFT JOIN sales_stats s ON s.item_alegra_id = b.item_alegra_id
            LEFT JOIN credit_stats c ON c.item_alegra_id = b.item_alegra_id
            LEFT JOIN purchase_stats ps ON ps.item_alegra_id = b.item_alegra_id
            LEFT JOIN last_purchase lp ON lp.item_alegra_id = b.item_alegra_id
            LEFT JOIN policy p
              ON p.item_alegra_id = b.item_alegra_id
             AND p.warehouse_alegra_id = b.warehouse_alegra_id
            """
        )
        result = self.session.execute(insert_sql, {"company_id": self.settings.company_id})
        count = result.rowcount or 0
        logger.info("gold_replenishment regenerada con %s filas", count)
        return count
