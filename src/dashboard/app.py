"""Streamlit dashboard for PUC Rate Case Tracker.

7-section interactive dashboard:
  1. National Overview (KPIs)
  2. Rate Case Explorer (filter/search)
  3. Utility Analysis (comparison)
  4. Rate Change Tracker (requested vs. approved)
  5. Geographic Map (choropleth)
  6. Timeline View (cases over time)
  7. Case Deep Dive (individual detail)

Dark theme: primaryColor="#0984E3", backgroundColor="#0E1117"
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from src.storage.database import (
        get_all_rate_cases,
        get_all_utilities,
        get_connection,
        get_documents_for_docket,
        get_rate_case_by_docket,
        get_stats,
        get_enrichment_stats,
        get_utility_operations,
        get_utility_emissions,
        get_utility_capacity,
        get_rate_case_impacts,
        get_utility_eia_links,
        init_db,
        DB_PATH,
    )
    _DB_IMPORTS_OK = True
except ImportError:
    _DB_IMPORTS_OK = False
    DB_PATH = PROJECT_ROOT / "data" / "puc_rate_cases.db"

    def init_db(): pass
    def get_connection(): return None
    def get_all_rate_cases(**kw): return []
    def get_all_utilities(**kw): return []
    def get_stats(**kw): return {}
    def get_enrichment_stats(**kw): return {}
    def get_utility_operations(**kw): return []
    def get_utility_emissions(**kw): return []
    def get_utility_capacity(**kw): return []
    def get_rate_case_impacts(**kw): return []
    def get_utility_eia_links(**kw): return []
    def get_documents_for_docket(**kw): return []
    def get_rate_case_by_docket(**kw): return None


# --- Page Config ---

st.set_page_config(
    page_title="PUC Rate Case Tracker",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Custom CSS ---

st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #0984E3;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1rem;
        color: #94A3B8;
        margin-bottom: 2rem;
    }
    .kpi-card {
        background-color: #1B2A4A;
        border-radius: 10px;
        padding: 1.2rem;
        text-align: center;
        border: 1px solid #2D4A7A;
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        color: #0984E3;
    }
    .kpi-label {
        font-size: 0.85rem;
        color: #94A3B8;
        margin-top: 0.3rem;
    }
    .footer {
        text-align: center;
        padding: 2rem 0 1rem 0;
        color: #64748B;
        font-size: 0.85rem;
        border-top: 1px solid #1B2A4A;
        margin-top: 3rem;
    }
    .footer a {
        color: #0984E3;
        text-decoration: none;
    }
</style>
""", unsafe_allow_html=True)


# --- Data Loading ---

@st.cache_data(ttl=300)
def load_data():
    """Load all rate case data from the database."""
    try:
        if not DB_PATH.exists():
            init_db()

        conn = get_connection()
        if conn is None:
            return pd.DataFrame(), [], {}, {}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), []

        cases = get_all_rate_cases(limit=10000, conn=conn)
        utilities = get_all_utilities(conn=conn)
        stats = get_stats(conn=conn, print_output=False)

        # Load enrichment data
        try:
            e_stats = get_enrichment_stats(conn=conn)
            operations = get_utility_operations(conn=conn)
            emissions = get_utility_emissions(conn=conn)
            capacity = get_utility_capacity(conn=conn)
            impacts = get_rate_case_impacts(conn=conn)
            eia_links = get_utility_eia_links(conn=conn)
        except Exception:
            e_stats = {}
            operations = []
            emissions = []
            capacity = []
            impacts = []
            eia_links = []

        try:
            conn.close()
        except Exception:
            pass

        df = pd.DataFrame(cases) if cases else pd.DataFrame()

        if not df.empty:
            df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
            df["decision_date"] = pd.to_datetime(df["decision_date"], errors="coerce")
            df["display_name"] = df["canonical_utility_name"].fillna(df["utility_name"])

        ops_df = pd.DataFrame(operations) if operations else pd.DataFrame()
        emi_df = pd.DataFrame(emissions) if emissions else pd.DataFrame()
        cap_df = pd.DataFrame(capacity) if capacity else pd.DataFrame()
        imp_df = pd.DataFrame(impacts) if impacts else pd.DataFrame()

        return df, utilities, stats, e_stats, ops_df, emi_df, cap_df, imp_df, eia_links
    except Exception:
        return pd.DataFrame(), [], {}, {}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), []


df, utilities, stats, e_stats, ops_df, emi_df, cap_df, imp_df, eia_links = load_data()


# --- Sidebar ---

st.sidebar.markdown('<div class="main-header">PUC Rate Case Tracker</div>', unsafe_allow_html=True)
st.sidebar.markdown(
    '<div class="sub-header">Cross-state utility rate case database</div>',
    unsafe_allow_html=True,
)

section = st.sidebar.radio(
    "Navigate",
    [
        "National Overview",
        "Rate Case Explorer",
        "Utility Analysis",
        "Rate Change Tracker",
        "Utility Profiles",
        "Consumer Impact",
        "Environmental Impact",
        "Capacity & Infrastructure",
        "Geographic Map",
        "Timeline View",
        "Case Deep Dive",
    ],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Data Sources:** OR PUC, MO PSC, CT PURA, GA PSC, PA PUC, CA CPUC, IN IURC, WA UTC + EIA + EPA eGRID"
)
_total_records = stats.get("total_rate_cases", 0) if stats else 0
_states_covered = len(stats.get("by_state", {})) if stats else 0
if _total_records > 0:
    st.sidebar.metric("Total Records", _total_records)
    st.sidebar.metric("States Covered", _states_covered)
    if e_stats:
        linked = e_stats.get("linked_utilities", 0)
        if linked:
            st.sidebar.metric("Utilities Linked to EIA", linked)
        emi = e_stats.get("emissions_records", 0)
        if emi:
            st.sidebar.metric("Emissions Records", emi)
else:
    st.sidebar.info("No data loaded yet. Run the pipeline to populate.")


# --- Helper Functions ---


def make_kpi_card(value: str, label: str) -> str:
    return f"""
    <div class="kpi-card">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
    </div>
    """


PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#E2E8F0"),
    margin=dict(l=40, r=40, t=40, b=40),
)

BLUE_PALETTE = ["#0984E3", "#74B9FF", "#0056A8", "#A3D8F4", "#003D73", "#B8E6FF"]


# ============================================================
# SECTION 1: NATIONAL OVERVIEW
# ============================================================

if section == "National Overview":
    st.markdown('<div class="main-header">National Overview</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">'
        "Aggregate metrics across all tracked state PUC rate cases"
        "</div>",
        unsafe_allow_html=True,
    )

    if df.empty:
        st.warning("No data loaded. Run the pipeline first: `python -m src.cli pipeline`")
    else:
        # KPI Row
        col1, col2, col3, col4, col5 = st.columns(5)

        total_cases = len(df)
        states_covered = df["state"].nunique()

        fin = stats.get("financial", {})
        total_requested = fin.get("total_requested_M", 0)
        avg_roe = fin.get("avg_roe_pct", 0)
        active_cases = len(df[df["status"] == "active"])

        with col1:
            st.markdown(make_kpi_card(f"{total_cases}", "Total Rate Cases"), unsafe_allow_html=True)
        with col2:
            st.markdown(make_kpi_card(f"{states_covered}", "States Tracked"), unsafe_allow_html=True)
        with col3:
            st.markdown(make_kpi_card(f"${total_requested:,.0f}M", "Total Requested"), unsafe_allow_html=True)
        with col4:
            st.markdown(make_kpi_card(f"{avg_roe:.1f}%", "Avg ROE"), unsafe_allow_html=True)
        with col5:
            st.markdown(make_kpi_card(f"{active_cases}", "Active Cases"), unsafe_allow_html=True)

        st.markdown("---")

        # Charts row
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("Cases by State")
            state_counts = df["state"].value_counts().reset_index()
            state_counts.columns = ["State", "Count"]
            fig = px.bar(
                state_counts, x="State", y="Count",
                color="Count", color_continuous_scale=["#003D73", "#0984E3", "#74B9FF"],
            )
            fig.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("Cases by Utility Type")
            type_counts = df["utility_type"].value_counts().reset_index()
            type_counts.columns = ["Type", "Count"]
            fig = px.pie(
                type_counts, values="Count", names="Type",
                color_discrete_sequence=BLUE_PALETTE,
                hole=0.4,
            )
            fig.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        # Quality distribution
        st.subheader("Quality Score Distribution")
        quality_df = df[df["quality_score"].notna()]
        if not quality_df.empty:
            fig = px.histogram(
                quality_df, x="quality_score", nbins=20,
                labels={"quality_score": "Quality Score", "count": "Count"},
                color_discrete_sequence=["#0984E3"],
            )
            fig.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)


# ============================================================
# SECTION 2: RATE CASE EXPLORER
# ============================================================

elif section == "Rate Case Explorer":
    st.markdown('<div class="main-header">Rate Case Explorer</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Search and filter rate cases across all states</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        st.warning("No data loaded.")
    else:
        # Filters
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            states = ["All"] + sorted(df["state"].dropna().unique().tolist())
            selected_state = st.selectbox("State", states)

        with col2:
            types = ["All"] + sorted(df["case_type"].dropna().unique().tolist())
            selected_type = st.selectbox("Case Type", types)

        with col3:
            statuses = ["All"] + sorted(df["status"].dropna().unique().tolist())
            selected_status = st.selectbox("Status", statuses)

        with col4:
            utility_types = ["All"] + sorted(df["utility_type"].dropna().unique().tolist())
            selected_utility_type = st.selectbox("Utility Type", utility_types)

        # Date range
        col_d1, col_d2, col_d3 = st.columns([2, 2, 2])
        with col_d1:
            search_text = st.text_input("Search utility name", "")

        filtered = df.copy()
        if selected_state != "All":
            filtered = filtered[filtered["state"] == selected_state]
        if selected_type != "All":
            filtered = filtered[filtered["case_type"] == selected_type]
        if selected_status != "All":
            filtered = filtered[filtered["status"] == selected_status]
        if selected_utility_type != "All":
            filtered = filtered[filtered["utility_type"] == selected_utility_type]
        if search_text:
            mask = (
                filtered["utility_name"].str.contains(search_text, case=False, na=False)
                | filtered["display_name"].str.contains(search_text, case=False, na=False)
                | filtered["docket_number"].str.contains(search_text, case=False, na=False)
            )
            filtered = filtered[mask]

        st.markdown(f"**{len(filtered)} rate cases found**")

        # Display table
        display_cols = [
            "docket_number", "display_name", "state", "case_type", "utility_type",
            "status", "filing_date", "decision_date",
            "requested_revenue_change", "approved_revenue_change",
            "return_on_equity", "quality_score",
        ]
        available_cols = [c for c in display_cols if c in filtered.columns]

        st.dataframe(
            filtered[available_cols].rename(columns={
                "docket_number": "Docket",
                "display_name": "Utility",
                "state": "State",
                "case_type": "Type",
                "utility_type": "Service",
                "status": "Status",
                "filing_date": "Filed",
                "decision_date": "Decided",
                "requested_revenue_change": "Requested ($M)",
                "approved_revenue_change": "Approved ($M)",
                "return_on_equity": "ROE (%)",
                "quality_score": "Quality",
            }),
            use_container_width=True,
            height=500,
        )


# ============================================================
# SECTION 3: UTILITY ANALYSIS
# ============================================================

elif section == "Utility Analysis":
    st.markdown('<div class="main-header">Utility Analysis</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Compare rate case patterns across utilities</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        st.warning("No data loaded.")
    else:
        # Utility summary
        utility_df = df.groupby("display_name").agg(
            total_cases=("docket_number", "count"),
            total_requested=("requested_revenue_change", "sum"),
            total_approved=("approved_revenue_change", "sum"),
            avg_roe=("return_on_equity", "mean"),
            states=("state", lambda x: ", ".join(sorted(x.unique()))),
        ).reset_index()

        utility_df = utility_df.sort_values("total_cases", ascending=False)

        st.subheader("Top Utilities by Number of Rate Cases")
        fig = px.bar(
            utility_df.head(15),
            x="display_name", y="total_cases",
            color="total_requested",
            color_continuous_scale=["#003D73", "#0984E3", "#74B9FF"],
            labels={"display_name": "Utility", "total_cases": "Rate Cases", "total_requested": "Total Requested ($M)"},
        )
        fig.update_layout(**PLOTLY_LAYOUT, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Utility Comparison Table")
        st.dataframe(
            utility_df.rename(columns={
                "display_name": "Utility",
                "total_cases": "Cases",
                "total_requested": "Total Requested ($M)",
                "total_approved": "Total Approved ($M)",
                "avg_roe": "Avg ROE (%)",
                "states": "State(s)",
            }),
            use_container_width=True,
        )

        # Approval rate by utility
        st.subheader("Approval Rate Analysis")
        approval_df = df.dropna(subset=["requested_revenue_change", "approved_revenue_change"]).copy()
        approval_df = approval_df[approval_df["requested_revenue_change"] != 0]
        if not approval_df.empty:
            approval_df["approval_pct"] = (
                approval_df["approved_revenue_change"] / approval_df["requested_revenue_change"] * 100
            )
            util_approval = approval_df.groupby("display_name")["approval_pct"].mean().reset_index()
            util_approval = util_approval.sort_values("approval_pct", ascending=True)

            fig = px.bar(
                util_approval.tail(15),
                x="approval_pct", y="display_name",
                orientation="h",
                labels={"approval_pct": "Avg Approval Rate (%)", "display_name": "Utility"},
                color="approval_pct",
                color_continuous_scale=["#FF6B6B", "#FFE66D", "#0984E3"],
            )
            fig.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)


# ============================================================
# SECTION 4: RATE CHANGE TRACKER
# ============================================================

elif section == "Rate Change Tracker":
    st.markdown('<div class="main-header">Rate Change Tracker</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Requested vs. approved revenue changes</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        st.warning("No data loaded.")
    else:
        rev_df = df.dropna(subset=["requested_revenue_change"]).copy()

        if not rev_df.empty:
            # Scatter: requested vs approved
            st.subheader("Requested vs. Approved Revenue Change")
            scatter_df = rev_df.dropna(subset=["approved_revenue_change"])

            if not scatter_df.empty:
                fig = px.scatter(
                    scatter_df,
                    x="requested_revenue_change",
                    y="approved_revenue_change",
                    color="state",
                    size=abs(scatter_df["requested_revenue_change"]).clip(lower=1),
                    hover_data=["docket_number", "display_name"],
                    labels={
                        "requested_revenue_change": "Requested ($M)",
                        "approved_revenue_change": "Approved ($M)",
                    },
                    color_discrete_sequence=BLUE_PALETTE,
                )
                # Add 100% approval line
                max_val = max(
                    scatter_df["requested_revenue_change"].max(),
                    scatter_df["approved_revenue_change"].max(),
                )
                fig.add_trace(
                    go.Scatter(
                        x=[0, max_val],
                        y=[0, max_val],
                        mode="lines",
                        line=dict(dash="dash", color="#64748B"),
                        name="100% Approval",
                        showlegend=True,
                    )
                )
                fig.update_layout(**PLOTLY_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)

            # Approval percentage distribution
            st.subheader("Approval Percentage Distribution")
            scatter_df_pct = scatter_df[scatter_df["requested_revenue_change"] != 0]
            if not scatter_df_pct.empty:
                scatter_df_pct = scatter_df_pct.copy()
                scatter_df_pct["approval_pct"] = (
                    scatter_df_pct["approved_revenue_change"] / scatter_df_pct["requested_revenue_change"] * 100
                )
                fig = px.histogram(
                    scatter_df_pct, x="approval_pct", nbins=20,
                    labels={"approval_pct": "Approval Rate (%)", "count": "Cases"},
                    color_discrete_sequence=["#0984E3"],
                )
                fig.update_layout(**PLOTLY_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)

            # By state average
            st.subheader("Average Revenue Change by State")
            state_rev = rev_df.groupby("state").agg(
                avg_requested=("requested_revenue_change", "mean"),
                avg_approved=("approved_revenue_change", "mean"),
            ).reset_index()

            fig = go.Figure()
            fig.add_trace(go.Bar(
                name="Requested", x=state_rev["state"], y=state_rev["avg_requested"],
                marker_color="#0984E3",
            ))
            fig.add_trace(go.Bar(
                name="Approved", x=state_rev["state"], y=state_rev["avg_approved"],
                marker_color="#74B9FF",
            ))
            fig.update_layout(**PLOTLY_LAYOUT, barmode="group")
            st.plotly_chart(fig, use_container_width=True)


# ============================================================
# SECTION 5: UTILITY PROFILES (EIA 861)
# ============================================================

elif section == "Utility Profiles":
    st.markdown('<div class="main-header">Utility Profiles</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">EIA Form 861 operational data — customers, revenue, pricing</div>',
        unsafe_allow_html=True,
    )

    if ops_df.empty:
        st.warning("No EIA 861 data loaded. Run: `python3 -m src.cli enrich`")
    else:
        # KPIs
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(make_kpi_card(f"{ops_df['eia_utility_id'].nunique():,}", "EIA Utilities"), unsafe_allow_html=True)
        with col2:
            total_cust = ops_df.groupby("eia_utility_id")["total_customers"].max().sum()
            st.markdown(make_kpi_card(f"{total_cust / 1e6:,.1f}M", "Total Customers"), unsafe_allow_html=True)
        with col3:
            avg_price = ops_df["residential_avg_price"].dropna().mean()
            st.markdown(make_kpi_card(f"{avg_price:.1f}¢", "Avg Res. Price"), unsafe_allow_html=True)
        with col4:
            total_rev = ops_df.groupby("eia_utility_id")["total_revenue"].max().sum()
            st.markdown(make_kpi_card(f"${total_rev / 1e6:,.0f}B", "Total Revenue"), unsafe_allow_html=True)

        st.markdown("---")

        # Ownership type breakdown
        col_left, col_right = st.columns(2)
        with col_left:
            st.subheader("Utilities by Ownership Type")
            own_counts = ops_df.drop_duplicates("eia_utility_id")["ownership_type"].value_counts().reset_index()
            own_counts.columns = ["Type", "Count"]
            fig = px.pie(own_counts, values="Count", names="Type", color_discrete_sequence=BLUE_PALETTE, hole=0.4)
            fig.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("Residential Price by State")
            if "state" in ops_df.columns:
                state_price = ops_df.dropna(subset=["residential_avg_price"]).groupby("state")["residential_avg_price"].mean().sort_values(ascending=False).head(20).reset_index()
                state_price.columns = ["State", "Avg Price (¢/kWh)"]
                fig = px.bar(state_price, x="State", y="Avg Price (¢/kWh)", color="Avg Price (¢/kWh)",
                             color_continuous_scale=["#003D73", "#0984E3", "#FF6B6B"])
                fig.update_layout(**PLOTLY_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)

        # Top utilities by customer count
        st.subheader("Largest Utilities by Customer Count")
        latest_year = ops_df["year"].max()
        latest = ops_df[ops_df["year"] == latest_year]
        top_util = latest.nlargest(15, "total_customers")[["utility_name", "state", "total_customers", "residential_avg_price", "ownership_type"]]
        st.dataframe(top_util.rename(columns={
            "utility_name": "Utility", "state": "State",
            "total_customers": "Customers", "residential_avg_price": "Res. Price (¢/kWh)",
            "ownership_type": "Ownership",
        }), use_container_width=True)


# ============================================================
# SECTION 6: CONSUMER IMPACT
# ============================================================

elif section == "Consumer Impact":
    st.markdown('<div class="main-header">Consumer Impact</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Rate case effects on customer bills</div>',
        unsafe_allow_html=True,
    )

    if imp_df.empty:
        st.warning("No impact data. Run: `python3 -m src.cli enrich`")
    else:
        # KPIs
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(make_kpi_card(f"{len(imp_df)}", "Cases with Impacts"), unsafe_allow_html=True)
        with col2:
            avg_monthly = imp_df["monthly_bill_impact"].mean()
            st.markdown(make_kpi_card(f"${avg_monthly:,.2f}", "Avg Monthly Impact"), unsafe_allow_html=True)
        with col3:
            max_monthly = imp_df["monthly_bill_impact"].max()
            st.markdown(make_kpi_card(f"${max_monthly:,.2f}", "Max Monthly Impact"), unsafe_allow_html=True)

        st.markdown("---")

        # Monthly bill impact distribution
        st.subheader("Monthly Bill Impact Distribution")
        fig = px.histogram(imp_df, x="monthly_bill_impact", nbins=30,
                           labels={"monthly_bill_impact": "Monthly Bill Impact ($)"},
                           color_discrete_sequence=["#0984E3"])
        fig.update_layout(**PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

        # Impact by case — merge with rate cases for context
        st.subheader("Largest Consumer Impacts")
        impact_display = imp_df.nlargest(15, "monthly_bill_impact")[
            ["docket_number", "total_customers", "monthly_bill_impact", "annual_bill_impact", "pct_of_avg_bill"]
        ]
        st.dataframe(impact_display.rename(columns={
            "docket_number": "Docket", "total_customers": "Customers",
            "monthly_bill_impact": "Monthly ($)", "annual_bill_impact": "Annual ($)",
            "pct_of_avg_bill": "% of Avg Bill",
        }), use_container_width=True)

        # Percentage of avg bill
        if "pct_of_avg_bill" in imp_df.columns:
            pct_data = imp_df.dropna(subset=["pct_of_avg_bill"])
            if not pct_data.empty:
                st.subheader("Impact as % of Average Bill")
                fig = px.histogram(pct_data, x="pct_of_avg_bill", nbins=20,
                                   labels={"pct_of_avg_bill": "% of Average Monthly Bill"},
                                   color_discrete_sequence=["#74B9FF"])
                fig.update_layout(**PLOTLY_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)


# ============================================================
# SECTION 7: ENVIRONMENTAL IMPACT (eGRID)
# ============================================================

elif section == "Environmental Impact":
    st.markdown('<div class="main-header">Environmental Impact</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">EPA eGRID utility emissions and generation mix</div>',
        unsafe_allow_html=True,
    )

    if emi_df.empty:
        st.warning("No eGRID data. Run: `python3 -m src.cli enrich`")
    else:
        # KPIs
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(make_kpi_card(f"{len(emi_df):,}", "Utility Records"), unsafe_allow_html=True)
        with col2:
            total_co2 = emi_df["co2_tons"].sum()
            st.markdown(make_kpi_card(f"{total_co2 / 1e6:,.0f}M", "Total CO2 (tons)"), unsafe_allow_html=True)
        with col3:
            avg_rate = emi_df["co2_rate_lbs_mwh"].dropna().mean()
            st.markdown(make_kpi_card(f"{avg_rate:,.0f}", "Avg CO2 Rate (lbs/MWh)"), unsafe_allow_html=True)
        with col4:
            total_gen = emi_df["net_generation_mwh"].sum()
            st.markdown(make_kpi_card(f"{total_gen / 1e9:,.1f}B", "Net Gen (MWh)"), unsafe_allow_html=True)

        st.markdown("---")

        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("CO2 Emission Rate Distribution")
            co2_data = emi_df.dropna(subset=["co2_rate_lbs_mwh"])
            if not co2_data.empty:
                fig = px.histogram(co2_data, x="co2_rate_lbs_mwh", nbins=30,
                                   labels={"co2_rate_lbs_mwh": "CO2 Rate (lbs/MWh)"},
                                   color_discrete_sequence=["#0984E3"])
                fig.update_layout(**PLOTLY_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("Average Generation Mix")
            mix_cols = ["coal_pct", "gas_pct", "nuclear_pct", "hydro_pct", "wind_pct", "solar_pct", "other_renewable_pct"]
            available_mix = [c for c in mix_cols if c in emi_df.columns]
            if available_mix:
                avg_mix = emi_df[available_mix].mean()
                mix_data = pd.DataFrame({"Source": [c.replace("_pct", "").title() for c in available_mix], "Percentage": avg_mix.values})
                mix_data = mix_data[mix_data["Percentage"] > 0]
                fig = px.pie(mix_data, values="Percentage", names="Source",
                             color_discrete_sequence=BLUE_PALETTE + ["#FF6B6B", "#FFE66D"], hole=0.4)
                fig.update_layout(**PLOTLY_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)

        # CO2 by state
        st.subheader("Average CO2 Rate by State")
        if "state" in emi_df.columns:
            state_co2 = emi_df.dropna(subset=["co2_rate_lbs_mwh"]).groupby("state")["co2_rate_lbs_mwh"].mean().sort_values(ascending=False).head(20).reset_index()
            state_co2.columns = ["State", "CO2 Rate (lbs/MWh)"]
            fig = px.bar(state_co2, x="State", y="CO2 Rate (lbs/MWh)", color="CO2 Rate (lbs/MWh)",
                         color_continuous_scale=["#0984E3", "#FFE66D", "#FF6B6B"])
            fig.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        # Top emitters
        st.subheader("Highest Emission Utilities")
        top_emitters = emi_df.nlargest(15, "co2_tons")[
            ["utility_name_egrid", "state", "co2_tons", "co2_rate_lbs_mwh", "net_generation_mwh"]
        ]
        st.dataframe(top_emitters.rename(columns={
            "utility_name_egrid": "Utility", "state": "State",
            "co2_tons": "CO2 (tons)", "co2_rate_lbs_mwh": "CO2 Rate",
            "net_generation_mwh": "Net Gen (MWh)",
        }), use_container_width=True)


# ============================================================
# SECTION 8: CAPACITY & INFRASTRUCTURE (EIA 860)
# ============================================================

elif section == "Capacity & Infrastructure":
    st.markdown('<div class="main-header">Capacity & Infrastructure</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">EIA Form 860 generation fleet analysis</div>',
        unsafe_allow_html=True,
    )

    if cap_df.empty:
        st.warning("No EIA 860 data. Run: `python3 -m src.cli enrich`")
    else:
        # KPIs
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(make_kpi_card(f"{len(cap_df):,}", "Utility Records"), unsafe_allow_html=True)
        with col2:
            total_cap = cap_df["total_capacity_mw"].sum()
            st.markdown(make_kpi_card(f"{total_cap / 1e3:,.0f} GW", "Total Capacity"), unsafe_allow_html=True)
        with col3:
            avg_age = cap_df["avg_generator_age"].dropna().mean()
            st.markdown(make_kpi_card(f"{avg_age:.0f} yrs", "Avg Fleet Age"), unsafe_allow_html=True)
        with col4:
            total_gens = cap_df["num_generators"].sum()
            st.markdown(make_kpi_card(f"{total_gens:,}", "Generators"), unsafe_allow_html=True)

        st.markdown("---")

        # Capacity mix
        st.subheader("National Capacity Mix")
        fuel_types = ["coal", "gas", "nuclear", "hydro", "wind", "solar", "other"]
        fuel_sums = {}
        for fuel in fuel_types:
            col_name = f"{fuel}_capacity_mw"
            if col_name in cap_df.columns:
                fuel_sums[fuel.title()] = cap_df[col_name].sum()

        if fuel_sums:
            mix_data = pd.DataFrame({"Fuel": fuel_sums.keys(), "Capacity (MW)": fuel_sums.values()})
            mix_data = mix_data[mix_data["Capacity (MW)"] > 0]
            fig = px.pie(mix_data, values="Capacity (MW)", names="Fuel",
                         color_discrete_sequence=BLUE_PALETTE + ["#FF6B6B", "#FFE66D"], hole=0.4)
            fig.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        # Fleet age distribution
        col_left, col_right = st.columns(2)
        with col_left:
            st.subheader("Generator Fleet Age Distribution")
            age_data = cap_df.dropna(subset=["avg_generator_age"])
            if not age_data.empty:
                fig = px.histogram(age_data, x="avg_generator_age", nbins=20,
                                   labels={"avg_generator_age": "Avg Fleet Age (years)"},
                                   color_discrete_sequence=["#0984E3"])
                fig.update_layout(**PLOTLY_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("Planned Additions vs Retirements")
            adds = cap_df["planned_additions_mw"].sum()
            retires = cap_df["planned_retirements_mw"].sum()
            if adds or retires:
                plan_data = pd.DataFrame({
                    "Category": ["Planned Additions", "Planned Retirements"],
                    "MW": [adds or 0, retires or 0],
                })
                fig = px.bar(plan_data, x="Category", y="MW", color="Category",
                             color_discrete_sequence=["#0984E3", "#FF6B6B"])
                fig.update_layout(**PLOTLY_LAYOUT, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        # Largest utilities by capacity
        st.subheader("Largest Utilities by Capacity")
        top_cap = cap_df.nlargest(15, "total_capacity_mw")[
            ["eia_utility_id", "total_capacity_mw", "num_plants", "num_generators", "avg_generator_age"]
        ]
        st.dataframe(top_cap.rename(columns={
            "eia_utility_id": "EIA ID", "total_capacity_mw": "Total MW",
            "num_plants": "Plants", "num_generators": "Generators",
            "avg_generator_age": "Avg Age",
        }), use_container_width=True)


# ============================================================
# SECTION 9: GEOGRAPHIC MAP
# ============================================================

elif section == "Geographic Map":
    st.markdown('<div class="main-header">Geographic Distribution</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Rate cases by state</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        st.warning("No data loaded.")
    else:
        # State-level aggregation
        state_agg = df.groupby("state").agg(
            cases=("docket_number", "count"),
            total_requested=("requested_revenue_change", "sum"),
            total_approved=("approved_revenue_change", "sum"),
            avg_roe=("return_on_equity", "mean"),
        ).reset_index()

        metric = st.selectbox(
            "Color by",
            ["cases", "total_requested", "total_approved", "avg_roe"],
            format_func=lambda x: {
                "cases": "Number of Cases",
                "total_requested": "Total Requested Revenue ($M)",
                "total_approved": "Total Approved Revenue ($M)",
                "avg_roe": "Average ROE (%)",
            }.get(x, x),
        )

        fig = px.choropleth(
            state_agg,
            locations="state",
            locationmode="USA-states",
            color=metric,
            scope="usa",
            color_continuous_scale=["#0E1117", "#003D73", "#0984E3", "#74B9FF"],
            labels={
                "cases": "Cases",
                "total_requested": "Requested ($M)",
                "total_approved": "Approved ($M)",
                "avg_roe": "Avg ROE (%)",
            },
        )
        fig.update_layout(
            geo=dict(
                bgcolor="rgba(0,0,0,0)",
                lakecolor="rgba(0,0,0,0)",
                landcolor="#1B2A4A",
            ),
            **PLOTLY_LAYOUT,
        )
        st.plotly_chart(fig, use_container_width=True)

        # State comparison table
        st.subheader("State Comparison")
        st.dataframe(
            state_agg.rename(columns={
                "state": "State",
                "cases": "Cases",
                "total_requested": "Total Requested ($M)",
                "total_approved": "Total Approved ($M)",
                "avg_roe": "Avg ROE (%)",
            }),
            use_container_width=True,
        )


# ============================================================
# SECTION 6: TIMELINE VIEW
# ============================================================

elif section == "Timeline View":
    st.markdown('<div class="main-header">Timeline Analysis</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Rate case filings and decisions over time</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        st.warning("No data loaded.")
    else:
        # Filing timeline
        timeline_df = df.dropna(subset=["filing_date"]).copy()

        if not timeline_df.empty:
            st.subheader("Filings Over Time")
            timeline_df["filing_year"] = timeline_df["filing_date"].dt.year
            year_counts = timeline_df.groupby(["filing_year", "state"]).size().reset_index(name="count")

            fig = px.bar(
                year_counts, x="filing_year", y="count", color="state",
                labels={"filing_year": "Year", "count": "Cases Filed", "state": "State"},
                color_discrete_sequence=BLUE_PALETTE,
            )
            fig.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

            # Case duration analysis
            st.subheader("Case Duration (Filing to Decision)")
            duration_df = df.dropna(subset=["filing_date", "decision_date"]).copy()
            if not duration_df.empty:
                duration_df["duration_days"] = (
                    duration_df["decision_date"] - duration_df["filing_date"]
                ).dt.days

                fig = px.histogram(
                    duration_df, x="duration_days", nbins=20,
                    labels={"duration_days": "Days", "count": "Cases"},
                    color_discrete_sequence=["#0984E3"],
                )
                fig.update_layout(**PLOTLY_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)

                # Average duration by state
                st.subheader("Average Case Duration by State")
                dur_by_state = duration_df.groupby("state")["duration_days"].mean().reset_index()
                dur_by_state = dur_by_state.sort_values("duration_days", ascending=True)

                fig = px.bar(
                    dur_by_state, x="state", y="duration_days",
                    labels={"state": "State", "duration_days": "Avg Days"},
                    color="duration_days",
                    color_continuous_scale=["#0984E3", "#FF6B6B"],
                )
                fig.update_layout(**PLOTLY_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)

            # Monthly filing pattern
            st.subheader("Filing Seasonality")
            timeline_df["filing_month"] = timeline_df["filing_date"].dt.month
            month_names = {
                1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
            }
            monthly = timeline_df["filing_month"].value_counts().sort_index().reset_index()
            monthly.columns = ["Month", "Count"]
            monthly["Month_Name"] = monthly["Month"].map(month_names)

            fig = px.bar(
                monthly, x="Month_Name", y="Count",
                labels={"Month_Name": "Month", "Count": "Cases Filed"},
                color_discrete_sequence=["#0984E3"],
            )
            fig.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)


# ============================================================
# SECTION 7: CASE DEEP DIVE
# ============================================================

elif section == "Case Deep Dive":
    st.markdown('<div class="main-header">Case Deep Dive</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Detailed view of individual rate cases</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        st.warning("No data loaded.")
    else:
        # Docket selector
        dockets = sorted(df["docket_number"].unique().tolist())

        if not dockets:
            st.info("No dockets available.")
        else:
            def _format_docket(x):
                match = df[df["docket_number"] == x]
                if match.empty:
                    return x
                return f"{x} - {match['display_name'].iloc[0]}"

            selected_docket = st.selectbox(
                "Select a docket number",
                dockets,
                format_func=_format_docket,
            )

            case_match = df[df["docket_number"] == selected_docket]
            if not selected_docket or case_match.empty:
                st.warning("Selected docket not found in data.")
            else:
                case_row = case_match.iloc[0]

                col1, col2 = st.columns([2, 1])

                with col1:
                    st.subheader(case_row.get("display_name", "Unknown Utility"))
                    st.markdown(f"**Docket:** {case_row.get('docket_number', 'N/A')}")
                    st.markdown(f"**State:** {case_row.get('state', 'N/A')}")
                    st.markdown(f"**Source:** {case_row.get('source', 'N/A')}")
                    if case_row.get("description"):
                        st.markdown(f"**Description:** {case_row['description']}")
                    if case_row.get("source_url"):
                        st.markdown(f"**Source URL:** [{case_row['source_url']}]({case_row['source_url']})")

                with col2:
                    quality = case_row.get("quality_score")
                    if quality is not None:
                        st.metric("Quality Score", f"{quality:.3f}")
                    st.metric("Status", case_row.get("status", "Unknown"))
                    st.metric("Case Type", case_row.get("case_type", "Unknown"))
                    st.metric("Utility Type", case_row.get("utility_type", "Unknown"))

                st.markdown("---")

                # Dates
                st.subheader("Timeline")
                col_d1, col_d2, col_d3 = st.columns(3)
                with col_d1:
                    filing = case_row.get("filing_date")
                    st.metric("Filing Date", str(filing)[:10] if pd.notna(filing) else "N/A")
                with col_d2:
                    decision = case_row.get("decision_date")
                    st.metric("Decision Date", str(decision)[:10] if pd.notna(decision) else "Pending")
                with col_d3:
                    if pd.notna(filing) and pd.notna(decision):
                        duration = (decision - filing).days
                        st.metric("Duration (days)", duration)
                    else:
                        st.metric("Duration (days)", "N/A")

                # Financials
                st.subheader("Financial Details")
                col_f1, col_f2, col_f3, col_f4 = st.columns(4)

                with col_f1:
                    req = case_row.get("requested_revenue_change")
                    st.metric(
                        "Requested Revenue Change",
                        f"${req:,.1f}M" if pd.notna(req) else "N/A",
                    )
                with col_f2:
                    app = case_row.get("approved_revenue_change")
                    st.metric(
                        "Approved Revenue Change",
                        f"${app:,.1f}M" if pd.notna(app) else "Pending",
                    )
                with col_f3:
                    rb = case_row.get("rate_base")
                    st.metric(
                        "Rate Base",
                        f"${rb:,.1f}M" if pd.notna(rb) else "N/A",
                    )
                with col_f4:
                    roe = case_row.get("return_on_equity")
                    st.metric(
                        "Return on Equity",
                        f"{roe:.2f}%" if pd.notna(roe) else "N/A",
                    )

                # Approval analysis
                if pd.notna(req) and pd.notna(app) and req != 0:
                    approval_pct = (app / req) * 100
                    st.markdown(f"**Approval Rate:** {approval_pct:.1f}% of requested amount")

                    # Visual bar
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        name="Requested", x=["Revenue Change"], y=[req],
                        marker_color="#0984E3",
                    ))
                    fig.add_trace(go.Bar(
                        name="Approved", x=["Revenue Change"], y=[app],
                        marker_color="#74B9FF",
                    ))
                    fig.update_layout(**PLOTLY_LAYOUT, barmode="group", height=300)
                    st.plotly_chart(fig, use_container_width=True)

                # Other cases from same utility
                st.subheader("Other Cases from this Utility")
                utility_name = case_row.get("display_name", "")
                other_cases = df[
                    (df["display_name"] == utility_name) & (df["docket_number"] != selected_docket)
                ]
                if not other_cases.empty:
                    st.dataframe(
                        other_cases[["docket_number", "state", "case_type", "status", "filing_date",
                                     "requested_revenue_change", "approved_revenue_change"]].rename(columns={
                            "docket_number": "Docket",
                            "state": "State",
                            "case_type": "Type",
                            "status": "Status",
                            "filing_date": "Filed",
                            "requested_revenue_change": "Requested ($M)",
                            "approved_revenue_change": "Approved ($M)",
                        }),
                        use_container_width=True,
                    )
                else:
                    st.info("No other cases found for this utility.")


# ============================================================
# FOOTER
# ============================================================

st.markdown(
    """
    <div class="footer">
        Built by Nathan Goldberg |
        <a href="mailto:nathanmauricegoldberg@gmail.com">nathanmauricegoldberg@gmail.com</a> |
        <a href="https://www.linkedin.com/in/nathan-goldberg-62a44522a/" target="_blank">LinkedIn</a>
    </div>
    """,
    unsafe_allow_html=True,
)
