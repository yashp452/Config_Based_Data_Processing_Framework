"""Retail Medallion Dashboard — reads Gold PostgreSQL tables (gold_schema)."""
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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

INDIGO  = "#6366f1"
GREEN   = "#10b981"
ROSE    = "#f43f5e"
AMBER   = "#f59e0b"
CYAN    = "#0ea5e9"
DARK    = "#0f172a"
SLATE   = "#64748b"

PALETTE = [INDIGO, GREEN, ROSE, AMBER, CYAN, "#a855f7", "#ec4899"]


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
    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* Overall background */
    .stApp { background: #f8fafc; }

    /* KPI cards */
    .kpi-card {
        background: #ffffff;
        border-radius: 14px;
        padding: 24px 20px;
        text-align: center;
        box-shadow: 0 2px 12px rgba(0,0,0,0.07);
        border-top: 4px solid #6366f1;
        transition: transform 0.2s;
    }
    .kpi-card:hover { transform: translateY(-2px); }
    .kpi-card .kpi-value {
        font-size: 2rem; font-weight: 800;
        color: #0f172a; margin: 8px 0 4px 0;
        letter-spacing: -1px;
    }
    .kpi-card .kpi-label {
        font-size: 0.72rem; color: #94a3b8;
        text-transform: uppercase; letter-spacing: 1.5px; font-weight: 600;
    }
    .kpi-card .kpi-sub {
        font-size: 0.78rem; color: #10b981;
        margin-top: 6px; font-weight: 500;
    }

    /* Green accent card */
    .kpi-card.green  { border-top-color: #10b981; }
    .kpi-card.rose   { border-top-color: #f43f5e; }
    .kpi-card.amber  { border-top-color: #f59e0b; }

    /* Section headers */
    .section-header {
        font-size: 0.9rem; font-weight: 700;
        color: #334155; letter-spacing: 0.3px;
        border-left: 4px solid #6366f1;
        padding-left: 10px; margin: 20px 0 10px 0;
        text-transform: uppercase;
    }

    /* Tab styling */
    div[data-testid="stTabs"] button {
        font-size: 0.88rem; font-weight: 600; color: #64748b;
        padding: 8px 16px;
    }
    div[data-testid="stTabs"] button[aria-selected="true"] {
        color: #6366f1; border-bottom-color: #6366f1;
    }

    /* Dashboard title */
    .dash-title {
        font-size: 1.8rem; font-weight: 800; color: #0f172a;
        letter-spacing: -0.5px;
    }
    .dash-subtitle {
        font-size: 0.82rem; color: #94a3b8; margin-top: 2px;
    }
    .dash-badge {
        display: inline-block;
        background: #ede9fe; color: #6366f1;
        font-size: 0.72rem; font-weight: 700;
        padding: 3px 10px; border-radius: 20px;
        letter-spacing: 0.5px; margin-left: 10px;
        vertical-align: middle;
    }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────

col_title, col_refresh = st.columns([5, 1])
with col_title:
    st.markdown(
        '<div class="dash-title">Retail Intelligence Hub'
        '<span class="dash-badge">LIVE</span></div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="dash-subtitle">Bronze → Silver → Gold medallion pipeline &nbsp;·&nbsp; Refreshed every 5 min</div>',
        unsafe_allow_html=True,
    )
with col_refresh:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.divider()

# ── Top-level KPIs ────────────────────────────────────────────────────────────

trend    = query("SELECT order_date, total_revenue, order_count, avg_order_value FROM gold_daily_trend ORDER BY order_date")
regional = query("SELECT region, segment, channel, total_revenue, order_count, avg_order_value, unique_customers FROM gold_regional_sales")

if not trend.empty:
    total_rev  = trend["total_revenue"].sum()
    total_ord  = int(trend["order_count"].sum())
    avg_ov     = trend["avg_order_value"].mean()
    total_cust = int(regional["unique_customers"].sum()) if not regional.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(f"""<div class="kpi-card">
        <div class="kpi-label">Total Revenue</div>
        <div class="kpi-value">${total_rev/1e6:.1f}M</div>
        <div class="kpi-sub">↑ All time</div></div>""", unsafe_allow_html=True)
    c2.markdown(f"""<div class="kpi-card green">
        <div class="kpi-label">Total Orders</div>
        <div class="kpi-value">{total_ord:,}</div>
        <div class="kpi-sub">↑ Across all days</div></div>""", unsafe_allow_html=True)
    c3.markdown(f"""<div class="kpi-card rose">
        <div class="kpi-label">Avg Order Value</div>
        <div class="kpi-value">${avg_ov:,.0f}</div>
        <div class="kpi-sub">Per transaction</div></div>""", unsafe_allow_html=True)
    c4.markdown(f"""<div class="kpi-card amber">
        <div class="kpi-label">Unique Customers</div>
        <div class="kpi-value">{total_cust:,}</div>
        <div class="kpi-sub">Across all regions</div></div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈  Sales Trend", "🏪  Store Performance", "👤  Customer Insights",
    "📦  Product Performance", "🗂️  Category Breakdown", "🌍  Regional Sales"
])

# ── Tab 1 — Sales Trend ───────────────────────────────────────────────────────

with tab1:
    if trend.empty:
        st.info("No data yet. Run the pipeline first.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown('<div class="section-header">Daily Revenue Trend</div>', unsafe_allow_html=True)
            fig = px.area(
                trend, x="order_date", y="total_revenue",
                color_discrete_sequence=[INDIGO],
                labels={"order_date": "Date", "total_revenue": "Revenue ($)"},
            )
            fig.update_traces(line_width=2, fillcolor="rgba(99,102,241,0.15)")
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=280,
                              plot_bgcolor="white", paper_bgcolor="white",
                              yaxis=dict(gridcolor="#f1f5f9"), xaxis=dict(gridcolor="#f1f5f9"))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<div class="section-header">Daily Order Volume</div>', unsafe_allow_html=True)
            fig = px.bar(
                trend, x="order_date", y="order_count",
                color_discrete_sequence=[DARK],
                labels={"order_date": "Date", "order_count": "Orders"},
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=280,
                              plot_bgcolor="white", paper_bgcolor="white",
                              yaxis=dict(gridcolor="#f1f5f9"), xaxis=dict(gridcolor="#f1f5f9"))
            st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="section-header">Average Order Value Over Time</div>', unsafe_allow_html=True)
        fig = px.line(
            trend, x="order_date", y="avg_order_value",
            color_discrete_sequence=[ROSE],
            markers=True,
            labels={"order_date": "Date", "avg_order_value": "Avg Order Value ($)"},
        )
        fig.update_traces(line_width=2.5, marker_size=5)
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=220,
                          plot_bgcolor="white", paper_bgcolor="white",
                          yaxis=dict(gridcolor="#f1f5f9"), xaxis=dict(gridcolor="#f1f5f9"))
        st.plotly_chart(fig, use_container_width=True)

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
            fig = px.bar(
                stores, x="total_revenue", y="store_name",
                orientation="h",
                color="store_type",
                color_discrete_map={"Online": INDIGO, "Offline": GREEN},
                labels={"total_revenue": "Revenue ($)", "store_name": ""},
                text="total_revenue",
            )
            fig.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
            fig.update_layout(margin=dict(l=0, r=80, t=10, b=0), height=420,
                              plot_bgcolor="white", paper_bgcolor="white",
                              legend_title="Type",
                              yaxis=dict(categoryorder="total ascending"),
                              xaxis=dict(gridcolor="#f1f5f9"))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<div class="section-header">Online vs Offline Split</div>', unsafe_allow_html=True)
            by_type = stores.groupby("store_type")["total_revenue"].sum().reset_index()
            fig = px.pie(
                by_type, values="total_revenue", names="store_type",
                color_discrete_sequence=[INDIGO, GREEN],
                hole=0.55,
            )
            fig.update_traces(textinfo="percent+label", textfont_size=13)
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=220,
                              showlegend=False, paper_bgcolor="white")
            st.plotly_chart(fig, use_container_width=True)

            st.markdown('<div class="section-header">Store Detail</div>', unsafe_allow_html=True)
            st.dataframe(
                stores[["store_name", "store_type", "total_revenue", "order_count"]]
                .rename(columns={"store_name": "Store", "store_type": "Type",
                                 "total_revenue": "Revenue", "order_count": "Orders"})
                .style.format({"Revenue": "${:,.0f}", "Orders": "{:,}"}),
                use_container_width=True, hide_index=True, height=200,
            )

# ── Tab 3 — Customer Insights ─────────────────────────────────────────────────

with tab3:
    customers = query("""
        SELECT customer_id, segment, region, total_spend, order_count, avg_order_value
        FROM gold_sales_by_customer ORDER BY total_spend DESC LIMIT 15
    """)
    seg = query("""
        SELECT segment,
               COUNT(*) AS customers,
               SUM(total_spend) AS total_revenue,
               AVG(total_spend) AS avg_clv
        FROM gold_sales_by_customer GROUP BY segment ORDER BY total_revenue DESC
    """)
    clv = query("SELECT AVG(total_spend) AS avg_clv FROM gold_sales_by_customer")

    if customers.empty:
        st.info("No data yet.")
    else:
        avg_clv = clv["avg_clv"].iloc[0] if not clv.empty else 0

        m1, m2, m3 = st.columns(3)
        m1.metric("Avg Customer Lifetime Value", f"${avg_clv:,.2f}")
        m2.metric("Total Segments", len(seg))
        m3.metric("Top Customer Spend", f"${customers['total_spend'].iloc[0]:,.0f}")

        st.markdown("<br>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)

        with col1:
            st.markdown('<div class="section-header">Top 15 Customers by Lifetime Spend</div>', unsafe_allow_html=True)
            fig = px.bar(
                customers, x="customer_id", y="total_spend",
                color="segment",
                color_discrete_sequence=PALETTE,
                labels={"customer_id": "Customer ID", "total_spend": "Total Spend ($)"},
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=320,
                              plot_bgcolor="white", paper_bgcolor="white",
                              xaxis=dict(tickangle=-30), yaxis=dict(gridcolor="#f1f5f9"))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<div class="section-header">Revenue & CLV by Segment</div>', unsafe_allow_html=True)
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=seg["segment"], y=seg["total_revenue"],
                name="Total Revenue", marker_color=INDIGO,
            ))
            fig.add_trace(go.Scatter(
                x=seg["segment"], y=seg["avg_clv"],
                name="Avg CLV", mode="markers+lines",
                marker=dict(color=ROSE, size=10), line=dict(color=ROSE, width=2),
                yaxis="y2",
            ))
            fig.update_layout(
                yaxis=dict(title="Revenue ($)", gridcolor="#f1f5f9"),
                yaxis2=dict(title="Avg CLV ($)", overlaying="y", side="right"),
                plot_bgcolor="white", paper_bgcolor="white",
                margin=dict(l=0, r=60, t=10, b=0), height=320,
                legend=dict(orientation="h", y=1.1),
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="section-header">Segment Breakdown</div>', unsafe_allow_html=True)
        st.dataframe(
            seg.rename(columns={"segment": "Segment", "customers": "Customers",
                                "total_revenue": "Total Revenue", "avg_clv": "Avg CLV"})
            .style.format({"Total Revenue": "${:,.0f}", "Avg CLV": "${:,.2f}", "Customers": "{:,}"}),
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
            fig = px.bar(
                products, x="product_id", y="total_revenue",
                color_discrete_sequence=[INDIGO],
                labels={"product_id": "Product ID", "total_revenue": "Revenue ($)"},
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=350,
                              plot_bgcolor="white", paper_bgcolor="white",
                              xaxis=dict(tickangle=-30), yaxis=dict(gridcolor="#f1f5f9"))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<div class="section-header">Revenue vs Quantity (Top 15)</div>', unsafe_allow_html=True)
            fig = px.scatter(
                products,
                x="total_quantity", y="total_revenue",
                size="order_count", color="total_revenue",
                color_continuous_scale=["#ede9fe", INDIGO],
                hover_name="product_id",
                labels={"total_quantity": "Units Sold", "total_revenue": "Revenue ($)",
                        "order_count": "Orders"},
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=350,
                              plot_bgcolor="white", paper_bgcolor="white",
                              coloraxis_showscale=False,
                              yaxis=dict(gridcolor="#f1f5f9"), xaxis=dict(gridcolor="#f1f5f9"))
            st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="section-header">Product Detail</div>', unsafe_allow_html=True)
        st.dataframe(
            products.rename(columns={"product_id": "Product", "total_revenue": "Revenue",
                                     "total_quantity": "Units Sold", "order_count": "Orders"})
            .style.format({"Revenue": "${:,.2f}", "Units Sold": "{:,}", "Orders": "{:,}"}),
            use_container_width=True, hide_index=True,
        )

# ── Tab 5 — Category Breakdown ────────────────────────────────────────────────

with tab5:
    cats = query("""
        SELECT category,
               SUM(total_revenue)  AS total_revenue,
               SUM(total_quantity) AS total_quantity,
               SUM(order_count)    AS order_count,
               AVG(avg_discount)   AS avg_discount
        FROM gold_category_performance
        WHERE category IS NOT NULL
        GROUP BY category ORDER BY total_revenue DESC
    """)

    if cats.empty:
        st.info("No data yet.")
    else:
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Categories", len(cats))
        m2.metric("Top Category", cats["category"].iloc[0])
        m3.metric("Top Category Revenue", f"${cats['total_revenue'].iloc[0]/1e6:.1f}M")

        st.markdown("<br>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown('<div class="section-header">Revenue by Category</div>', unsafe_allow_html=True)
            fig = px.bar(
                cats, x="category", y="total_revenue",
                color="category", color_discrete_sequence=PALETTE,
                labels={"category": "", "total_revenue": "Revenue ($)"},
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=300,
                              plot_bgcolor="white", paper_bgcolor="white",
                              showlegend=False, yaxis=dict(gridcolor="#f1f5f9"))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<div class="section-header">Units Sold by Category</div>', unsafe_allow_html=True)
            fig = px.bar(
                cats, x="category", y="total_quantity",
                color="category", color_discrete_sequence=PALETTE,
                labels={"category": "", "total_quantity": "Units Sold"},
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=300,
                              plot_bgcolor="white", paper_bgcolor="white",
                              showlegend=False, yaxis=dict(gridcolor="#f1f5f9"))
            st.plotly_chart(fig, use_container_width=True)

        with col3:
            st.markdown('<div class="section-header">Revenue Share</div>', unsafe_allow_html=True)
            fig = px.pie(
                cats, values="total_revenue", names="category",
                color_discrete_sequence=PALETTE, hole=0.45,
            )
            fig.update_traces(textinfo="percent+label", textfont_size=11)
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=300,
                              showlegend=False, paper_bgcolor="white")
            st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="section-header">Category Detail</div>', unsafe_allow_html=True)
        st.dataframe(
            cats.rename(columns={"category": "Category", "total_revenue": "Revenue",
                                 "total_quantity": "Units Sold", "order_count": "Orders",
                                 "avg_discount": "Avg Discount"})
            .style.format({"Revenue": "${:,.0f}", "Units Sold": "{:,}",
                           "Orders": "{:,}", "Avg Discount": "{:.1%}"}),
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

        m1, m2, m3 = st.columns(3)
        m1.metric("Regions", len(by_region))
        m2.metric("Top Region", by_region["region"].iloc[0])
        m3.metric("Top Region Revenue", f"${by_region['total_revenue'].iloc[0]/1e6:.1f}M")

        st.markdown("<br>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown('<div class="section-header">Revenue by Region</div>', unsafe_allow_html=True)
            fig = px.bar(
                by_region, x="region", y="total_revenue",
                color="region", color_discrete_sequence=PALETTE,
                text="total_revenue",
                labels={"region": "", "total_revenue": "Revenue ($)"},
            )
            fig.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
            fig.update_layout(margin=dict(l=0, r=0, t=30, b=0), height=320,
                              plot_bgcolor="white", paper_bgcolor="white",
                              showlegend=False, yaxis=dict(gridcolor="#f1f5f9"))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<div class="section-header">Online vs In-Store</div>', unsafe_allow_html=True)
            fig = px.pie(
                by_channel, values="total_revenue", names="channel",
                color_discrete_sequence=[INDIGO, GREEN], hole=0.5,
            )
            fig.update_traces(textinfo="percent+label", textfont_size=13)
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=320,
                              showlegend=False, paper_bgcolor="white")
            st.plotly_chart(fig, use_container_width=True)

        with col3:
            st.markdown('<div class="section-header">Customers by Region</div>', unsafe_allow_html=True)
            fig = px.bar(
                by_region, x="region", y="unique_customers",
                color="region", color_discrete_sequence=PALETTE,
                labels={"region": "", "unique_customers": "Customers"},
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=320,
                              plot_bgcolor="white", paper_bgcolor="white",
                              showlegend=False, yaxis=dict(gridcolor="#f1f5f9"))
            st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="section-header">Region × Segment Revenue Breakdown</div>', unsafe_allow_html=True)
        pivot = regional.groupby(["region", "segment"])["total_revenue"].sum().unstack(fill_value=0)
        fig = px.imshow(
            pivot,
            color_continuous_scale=["#ede9fe", INDIGO],
            text_auto="$.2s",
            aspect="auto",
            labels=dict(x="Segment", y="Region", color="Revenue"),
        )
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=250,
                          paper_bgcolor="white", plot_bgcolor="white")
        st.plotly_chart(fig, use_container_width=True)
