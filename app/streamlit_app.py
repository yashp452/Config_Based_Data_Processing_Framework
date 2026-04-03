"""Retail Medallion Dashboard — reads Gold PostgreSQL tables (gold_schema)."""
import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# ── Connection ────────────────────────────────────────────────────────────────

DB_URL = "postgresql://{user}:{pw}@{host}:{port}/{db}".format(
    user=os.getenv("POSTGRES_USER", "postgres"),
    pw=os.getenv("POSTGRES_PASSWORD", "postgres"),
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    db=os.getenv("POSTGRES_DB", "gold"),
)


@st.cache_resource
def get_engine():
    return create_engine(DB_URL, connect_args={"options": "-csearch_path=gold_schema"})


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    try:
        return pd.read_sql(text(sql), get_engine())
    except Exception as exc:
        st.error(f"Query failed: {exc}")
        return pd.DataFrame()


# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Retail Intelligence Hub",
    layout="wide",
    page_icon="🛍️",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    /* Metric cards */
    .metric-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 22px 18px;
        text-align: center;
        box-shadow: 0 1px 4px rgba(0,0,0,0.07);
    }
    .metric-card .value {
        font-size: 1.85rem; font-weight: 700;
        color: #0f172a; margin: 6px 0;
    }
    .metric-card .label {
        font-size: 0.78rem; color: #64748b;
        text-transform: uppercase; letter-spacing: 1.2px;
    }
    .metric-card .delta {
        font-size: 0.8rem; color: #10b981; margin-top: 4px;
    }
    /* Section headers */
    .section-header {
        font-size: 0.95rem; font-weight: 600;
        color: #334155; letter-spacing: 0.3px;
        border-left: 3px solid #6366f1;
        padding-left: 10px; margin: 18px 0 8px 0;
    }
    /* Tab styling */
    div[data-testid="stTabs"] button {
        font-size: 0.88rem; font-weight: 500; color: #475569;
    }
    div[data-testid="stTabs"] button[aria-selected="true"] {
        color: #6366f1; border-bottom-color: #6366f1;
    }
    /* Main header */
    .dash-title {
        font-size: 1.6rem; font-weight: 700; color: #0f172a; margin-bottom: 2px;
    }
    .dash-subtitle {
        font-size: 0.82rem; color: #94a3b8;
    }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────

st.markdown('<div class="dash-title">Retail Intelligence Hub</div>', unsafe_allow_html=True)
st.markdown('<div class="dash-subtitle">Bronze → Silver → Gold medallion pipeline &nbsp;·&nbsp; Data refreshed every 5 min</div>', unsafe_allow_html=True)
st.divider()

# ── Top-level KPIs ────────────────────────────────────────────────────────────

trend    = query("SELECT order_date, total_revenue, order_count, avg_order_value FROM gold_daily_trend ORDER BY order_date")
regional = query("SELECT region, segment, channel, total_revenue, order_count, avg_order_value, unique_customers FROM gold_regional_sales")

if not trend.empty:
    total_rev  = trend["total_revenue"].sum()
    total_ord  = trend["order_count"].sum()
    avg_ov     = trend["avg_order_value"].mean()
    total_cust = regional["unique_customers"].sum() if not regional.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(f"""<div class="metric-card">
        <div class="label">Total Revenue</div>
        <div class="value">${total_rev/1e6:.1f}M</div>
        <div class="delta">All time</div></div>""", unsafe_allow_html=True)
    c2.markdown(f"""<div class="metric-card">
        <div class="label">Total Orders</div>
        <div class="value">{int(total_ord):,}</div>
        <div class="delta">500K orders</div></div>""", unsafe_allow_html=True)
    c3.markdown(f"""<div class="metric-card">
        <div class="label">Avg Order Value</div>
        <div class="value">${avg_ov:,.0f}</div>
        <div class="delta">Per order</div></div>""", unsafe_allow_html=True)
    c4.markdown(f"""<div class="metric-card">
        <div class="label">Unique Customers</div>
        <div class="value">{int(total_cust):,}</div>
        <div class="delta">Across all regions</div></div>""", unsafe_allow_html=True)

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈 Sales Trend", "🏪 Store Performance", "👤 Customer Insights",
    "📦 Product Performance", "🗂️ Category Breakdown", "🌍 Regional Sales"
])

# ── Tab 1 — Sales Trend ───────────────────────────────────────────────────────

with tab1:
    if trend.empty:
        st.info("No data yet. Run the pipeline first.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown('<div class="section-header">Daily Revenue Trend</div>', unsafe_allow_html=True)
            st.area_chart(trend.set_index("order_date")["total_revenue"], height=280, color="#6366f1")

        with col2:
            st.markdown('<div class="section-header">Daily Order Volume</div>', unsafe_allow_html=True)
            st.bar_chart(trend.set_index("order_date")["order_count"], height=280, color="#0f172a")

        st.markdown('<div class="section-header">Average Order Value Over Time</div>', unsafe_allow_html=True)
        st.line_chart(trend.set_index("order_date")["avg_order_value"], height=220, color="#f43f5e")

# ── Tab 2 — Store Performance ─────────────────────────────────────────────────

with tab2:
    stores = query("""
        SELECT store_id, store_name, store_type,
               SUM(total_revenue) AS total_revenue,
               SUM(order_count)   AS order_count,
               AVG(avg_order_value) AS avg_order_value
        FROM gold_sales_by_store
        GROUP BY store_id, store_name, store_type
        ORDER BY total_revenue DESC
        LIMIT 15
    """)

    if stores.empty:
        st.info("No data yet.")
    else:
        col1, col2 = st.columns([3, 2])

        with col1:
            st.markdown('<div class="section-header">Top 15 Stores by Revenue</div>', unsafe_allow_html=True)
            st.bar_chart(stores.set_index("store_name")["total_revenue"], height=380, color="#6366f1")

        with col2:
            st.markdown('<div class="section-header">Online vs Offline Split</div>', unsafe_allow_html=True)
            by_type = stores.groupby("store_type")["total_revenue"].sum().reset_index()
            st.dataframe(
                by_type.style.format({"total_revenue": "${:,.0f}"}),
                use_container_width=True, hide_index=True,
            )
            st.markdown('<div class="section-header">Store Detail</div>', unsafe_allow_html=True)
            st.dataframe(
                stores[["store_name", "store_type", "total_revenue", "order_count", "avg_order_value"]]
                .style.format({"total_revenue": "${:,.0f}", "avg_order_value": "${:,.2f}", "order_count": "{:,}"}),
                use_container_width=True, hide_index=True,
            )

# ── Tab 3 — Customer Insights ─────────────────────────────────────────────────

with tab3:
    customers = query("""
        SELECT customer_id, segment, region, total_spend, order_count, avg_order_value
        FROM gold_sales_by_customer ORDER BY total_spend DESC LIMIT 10
    """)
    seg = query("""
        SELECT segment, COUNT(*) AS customers, SUM(total_spend) AS total_revenue, AVG(total_spend) AS avg_clv
        FROM gold_sales_by_customer GROUP BY segment ORDER BY total_revenue DESC
    """)
    clv = query("SELECT AVG(total_spend) AS avg_clv FROM gold_sales_by_customer")

    if customers.empty:
        st.info("No data yet.")
    else:
        avg_clv = clv["avg_clv"].iloc[0] if not clv.empty else 0

        col1, col2, col3 = st.columns(3)
        col1.metric("Avg Customer Lifetime Value", f"${avg_clv:,.2f}")
        col2.metric("Total Segments", len(seg))
        col3.metric("Top Customer Spend", f"${customers['total_spend'].iloc[0]:,.0f}")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-header">Top 10 Customers by Lifetime Spend</div>', unsafe_allow_html=True)
            st.bar_chart(customers.set_index("customer_id")["total_spend"], height=300, color="#6366f1")

        with col2:
            st.markdown('<div class="section-header">Revenue by Segment</div>', unsafe_allow_html=True)
            st.bar_chart(seg.set_index("segment")["total_revenue"], height=300, color="#10b981")

        st.markdown('<div class="section-header">Segment Breakdown</div>', unsafe_allow_html=True)
        st.dataframe(
            seg.style.format({"total_revenue": "${:,.0f}", "avg_clv": "${:,.2f}", "customers": "{:,}"}),
            use_container_width=True, hide_index=True,
        )

# ── Tab 4 — Product Performance ───────────────────────────────────────────────

with tab4:
    products = query("""
        SELECT product_id, total_quantity, total_revenue, order_count
        FROM gold_product_performance ORDER BY total_revenue DESC LIMIT 15
    """)

    if products.empty:
        st.info("No data yet.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-header">Top 15 Products by Revenue</div>', unsafe_allow_html=True)
            st.bar_chart(products.set_index("product_id")["total_revenue"], height=350, color="#6366f1")

        with col2:
            st.markdown('<div class="section-header">Top 15 Products by Quantity Sold</div>', unsafe_allow_html=True)
            st.bar_chart(products.set_index("product_id")["total_quantity"], height=350, color="#f59e0b")

        st.markdown('<div class="section-header">Product Detail</div>', unsafe_allow_html=True)
        st.dataframe(
            products.style.format({
                "total_revenue": "${:,.2f}",
                "total_quantity": "{:,}",
                "order_count": "{:,}",
            }),
            use_container_width=True, hide_index=True,
        )

# ── Tab 5 — Category Breakdown ────────────────────────────────────────────────

with tab5:
    cats = query("""
        SELECT category, SUM(total_revenue) AS total_revenue,
               SUM(total_quantity) AS total_quantity,
               SUM(order_count) AS order_count,
               AVG(avg_discount) AS avg_discount
        FROM gold_category_performance
        WHERE category IS NOT NULL
        GROUP BY category ORDER BY total_revenue DESC
    """)

    if cats.empty:
        st.info("No data yet.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Categories", len(cats))
        col2.metric("Top Category", cats["category"].iloc[0])
        col3.metric("Top Category Revenue", f"${cats['total_revenue'].iloc[0]/1e6:.1f}M")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-header">Revenue by Category</div>', unsafe_allow_html=True)
            st.bar_chart(cats.set_index("category")["total_revenue"], height=320, color="#0ea5e9")

        with col2:
            st.markdown('<div class="section-header">Units Sold by Category</div>', unsafe_allow_html=True)
            st.bar_chart(cats.set_index("category")["total_quantity"], height=320, color="#f59e0b")

        st.markdown('<div class="section-header">Category Detail</div>', unsafe_allow_html=True)
        st.dataframe(
            cats.style.format({
                "total_revenue": "${:,.0f}",
                "total_quantity": "{:,}",
                "order_count": "{:,}",
                "avg_discount": "{:.1%}",
            }),
            use_container_width=True, hide_index=True,
        )

# ── Tab 6 — Regional Sales ────────────────────────────────────────────────────

with tab6:
    if regional.empty:
        st.info("No data yet.")
    else:
        by_region = regional.groupby("region").agg(
            total_revenue=("total_revenue", "sum"),
            order_count=("order_count", "sum"),
            unique_customers=("unique_customers", "sum"),
        ).reset_index().sort_values("total_revenue", ascending=False)

        by_channel = regional.groupby("channel").agg(
            total_revenue=("total_revenue", "sum"),
            order_count=("order_count", "sum"),
        ).reset_index()

        col1, col2, col3 = st.columns(3)
        col1.metric("Regions", len(by_region))
        col2.metric("Top Region", by_region["region"].iloc[0])
        col3.metric("Top Region Revenue", f"${by_region['total_revenue'].iloc[0]/1e6:.1f}M")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-header">Revenue by Region</div>', unsafe_allow_html=True)
            st.bar_chart(by_region.set_index("region")["total_revenue"], height=300, color="#6366f1")

        with col2:
            st.markdown('<div class="section-header">Online vs In-Store Revenue</div>', unsafe_allow_html=True)
            st.bar_chart(by_channel.set_index("channel")["total_revenue"], height=300, color="#10b981")

        st.markdown('<div class="section-header">Region × Segment Breakdown</div>', unsafe_allow_html=True)
        pivot = regional.groupby(["region", "segment"])["total_revenue"].sum().unstack(fill_value=0)
        st.dataframe(
            pivot.style.format("${:,.0f}"),
            use_container_width=True,
        )
